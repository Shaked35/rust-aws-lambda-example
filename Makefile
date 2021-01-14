TARGET = x86_64-unknown-linux-musl
ARTIFACT_DIR = target/$(TARGET)/release
PACKAGE_DIR = target/$(TARGET)/package

check:
	cargo check

clippy:
	cargo clippy

debug:
	cross build --target "x86_64-unknown-linux-musl"

build:
	cross build --release --target "x86_64-unknown-linux-musl"

package-%: build $(ARTIFACT_DIR)/%
	mkdir -p $(PACKAGE_DIR)/$*
	if [ -f "$(PACKAGE_DIR)/$*.zip" ]; then rm $(PACKAGE_DIR)/$*.zip; fi
	cp $(ARTIFACT_DIR)/$* $(PACKAGE_DIR)/$*/bootstrap
	zip -j -9 $(PACKAGE_DIR)/$*.zip $(PACKAGE_DIR)/$*/bootstrap

all-package: package-count package-pull package-decide

deploy-%: package-%
	aws lambda update-function-code --function-name "adwords_$*" --zip-file fileb://$(PACKAGE_DIR)/$*.zip

all-deploy: deploy-data_lake deploy-reports deploy-config_builder deploy-update_google_report_types

