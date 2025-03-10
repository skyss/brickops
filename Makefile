.PHONY: build-package

COLOUR_GREEN=\033[0;32m
COLOUR_RED=\033[0;31m
COLOUR_BLUE=\033[0;34m
END_COLOUR=\033[0m

build-package:
	@echo "$(COLOUR_GREEN)Building package ... $(END_COLOUR)"
	@uv build
	@echo "$(COLOUR_GREEN)Finished building package... $(END_COLOUR)"
