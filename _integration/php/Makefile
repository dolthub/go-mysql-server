vendor/autoload.php:
	composer install

dependencies: vendor/autoload.php

test: dependencies
	./vendor/bin/phpunit --bootstrap=vendor/autoload.php tests/MySQLTest

.PHONY: test