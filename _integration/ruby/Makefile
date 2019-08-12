vendor/bundle:
	gem install bundler --version=1.16.5
	bundler install --path vendor/bundle

dependencies: vendor/bundle

test: dependencies
	bundler exec ruby mysql_test.rb

.PHONY: test
