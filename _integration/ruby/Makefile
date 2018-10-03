vendor/bundle:
	bundler install --path vendor/bundle

dependencies: vendor/bundle

test: dependencies
	bundler exec ruby mysql_test.rb

.PHONY: test