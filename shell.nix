{ pkgs ? import <nixpkgs> {
    config = {
      allowUnfree = true;
      cudaSupport = true;
    };
  }
}:

pkgs.mkShell {
  name = "NetSec";

  buildInputs = with pkgs; [
    # CUDA toolkit (adjust version as needed: cudaPackages_11 / cudaPackages_12)
    cudaPackages.cudatoolkit
    cudaPackages.cudnn

    # Python + uv
    python312
    uv

    # Common native deps useful for ML/CUDA projects
    stdenv.cc.cc.lib   # libstdc++
    zlib
    libGL
  ];

  shellHook = ''
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  CUDA + uv Nix Shell"
    echo "  CUDA version: $(nvcc --version 2>/dev/null | grep release | awk '{print $5}' | tr -d , || echo 'nvcc not found')"
    echo "  uv   version: $(uv --version)"
    echo "  Python:       $(python --version)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Create venv with uv if it doesn't exist yet
    if [ ! -d ".venv" ]; then
      echo "→ Creating venv with uv..."
      uv venv .venv --python python3.11
    fi

    # Activate it
    source .venv/bin/activate

    # Point uv / pip at the CUDA-aware PyTorch index
    export UV_EXTRA_INDEX_URL="https://download.pytorch.org/whl/cu121"

    echo "→ Venv active: $VIRTUAL_ENV"
    echo "  Install packages with: uv pip install torch torchvision torchaudio"
  '';
}
