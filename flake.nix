{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem(system: 
      let
        pkgs = nixpkgs.legacyPackages.${system};

        go-migrate = pkgs.go-migrate.overrideAttrs(oldAttrs: {
            tags = ["postgres"];
        });
      in
      {
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            go-migrate
            sqlc
          ];
        };
      });
}
