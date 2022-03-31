VERSION=latest
NAME=consul-rr
USER=registry.gitlab.com/nextensio/routereflector
image=$(shell docker images $(USER)/$(NAME):$(VERSION) -q)

.PHONY: all
all: build 

.PHONY: build
build:
	cp ~/.ssh/gitlab_rsa files/
	docker build -f Dockerfile.build -t $(USER)/$(NAME):$(VERSION) .
	docker create $(USER)/$(NAME):$(VERSION)
	rm files/gitlab_rsa
.PHONY: clean
clean:
	-docker rmi $(image)
