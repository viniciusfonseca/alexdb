target:
	cargo build --release

image:
	docker build . -t distanteagle16/alexdb

push-image:
	docker push distanteagle16/alexdb