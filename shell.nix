{ use_zsh ? "true" }:
let
  # nixos-25.05 stable as of 20 Sep 2025. See: https://status.nixos.org
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/archive/d2ed99647a4b195f0bcc440f76edfa10aeb3b743.tar.gz";
  pkgs = import nixpkgs { config = {}; overlays = []; };
in

pkgs.mkShell {
  packages = [
   pkgs.clojure
   pkgs.babashka
   pkgs.clj-kondo
   pkgs.cljfmt
   pkgs.temurin-bin
   pkgs.zsh
  ];

  shellHook = ''
    if [[ ${use_zsh} = true ]]
    then
      exec zsh
    fi
  '';
}
