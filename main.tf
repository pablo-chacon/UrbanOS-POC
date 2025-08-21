terraform {
  required_version = ">= 1.3.0"
}

# local execution.
provider "local" {}

# Variables
variable "repo_url" {
  description = "UrbanOS-POC Git repo (HTTPS for keyless clone)"
  type        = string
  default     = "https://github.com/pablo-chacon/UrbanOS-POC.git"
}

variable "repo_branch" {
  description = "Git branch or tag to checkout"
  type        = string
  default     = "main"
}

variable "repo_dir" {
  description = "Local folder name for the repo"
  type        = string
  default     = "UrbanOS-POC"
}

variable "compose_build_no_cache" {
  description = "If true, build with --no-cache"
  type        = bool
  default     = false
}

# Clone / update the repo
resource "null_resource" "clone_or_update" {
  triggers = {
    repo_url   = var.repo_url
    repo_dir   = var.repo_dir
    repo_branch= var.repo_branch
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command = <<-EOT
      set -euo pipefail

      if ! command -v git >/dev/null 2>&1; then
        echo "ERROR: git is required." >&2; exit 1
      fi

      if [ -d "${var.repo_dir}/.git" ]; then
        echo "[git] Updating existing repo in ${var.repo_dir}"
        git -C "${var.repo_dir}" fetch --all --prune
        git -C "${var.repo_dir}" checkout "${var.repo_branch}"
        git -C "${var.repo_dir}" pull --ff-only origin "${var.repo_branch}"
      else
        echo "[git] Cloning ${var.repo_url} -> ${var.repo_dir}"
        git clone --branch "${var.repo_branch}" --depth 1 "${var.repo_url}" "${var.repo_dir}"
      fi
    EOT
  }
}

# Ensure .env exists (key-safe). Prefer local .env in the repo dir
resource "null_resource" "prepare_env" {
  depends_on = [null_resource.clone_or_update]

  triggers = {
    repo_dir = var.repo_dir
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command = <<-EOT
      set -euo pipefail
      cd "${var.repo_dir}"

      if [ ! -f ".env" ]; then
        if [ -f ".env.dev" ]; then
          echo "[env] Creating .env from .env.dev (template)"
          cp .env.dev .env
        else
          echo "ERROR: No .env found and no .env.dev template available." >&2
          echo "Create ${var.repo_dir}/.env (or add .env.dev) before continuing." >&2
          exit 1
        fi
      else
        echo "[env] .env already present; leaving as-is."
      fi

      # Sanity check for docker compose file
      if [ ! -f "docker-compose.yml" ]; then
        echo "ERROR: docker-compose.yml not found in ${var.repo_dir}" >&2
        exit 1
      fi
    EOT
  }
}

# Build + Up stack
resource "null_resource" "compose_up" {
  depends_on = [null_resource.prepare_env]

  triggers = {
    repo_dir             = var.repo_dir
    compose_build_no_cache = tostring(var.compose_build_no_cache)
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command = <<-EOT
      set -euo pipefail

      if ! command -v docker >/dev/null 2>&1; then
        echo "ERROR: docker is required." >&2; exit 1
      fi
      # docker compose (v2) required
      if ! docker compose version >/dev/null 2>&1; then
        echo "ERROR: docker compose v2 is required." >&2; exit 1
      fi

      cd "${var.repo_dir}"

      export DOCKER_BUILDKIT=1

      if [ "${var.compose_build_no_cache}" = "true" ]; then
        echo "[compose] Building images (no cache)…"
        docker compose build --no-cache
      else
        echo "[compose] Building images…"
        docker compose build
      fi

      echo "[compose] Starting stack…"
      docker compose up -d

      echo "[compose] Stack status:"
      docker compose ps
    EOT
  }

  # Bring stack down on 'terraform destroy'
  provisioner "local-exec" {
    when       = destroy
    interpreter = ["/bin/bash", "-c"]
    command    = <<-EOT
      set -euo pipefail
      if [ -d "${var.repo_dir}" ] && [ -f "${var.repo_dir}/docker-compose.yml" ]; then
        echo "[compose] Stopping stack and removing volumes…"
        (cd "${var.repo_dir}" && docker compose down -v || true)
      fi
    EOT
  }
}

# Outputs
output "repo_path" {
  value       = abspath(var.repo_dir)
  description = "Local path of the UrbanOS-POC repository."
}

output "status" {
  value       = "UrbanOS-POC cloned/updated and docker compose stack started."
  description = "Human-readable status."
}
