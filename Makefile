#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = NC_ToteProject
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD := $(shell pwd)
PYTHONPATH := ${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

# Prints environment variables to ensure correct python path set
print-vars:
	@ echo "WD is $(WD)"
	@ echo "PYTHONPATH is $(PYTHONPATH)"

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up venv."
	( \
	    $(PYTHON_INTERPRETER) -m venv venv; \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install pytest-cov)

## Install flake8
flake8:
	$(call execute_in_env, $(PIP) install flake8)

## Install mypy
mypy:
	$(call execute_in_env, $(PIP) install mypy)

## Set up dev requirements (bandit, black, coverage, flake8 & mypy)
dev-setup: bandit black coverage flake8 mypy

# Build / Run

## Run the security test (bandit)
security-test:
	$(call execute_in_env, bandit -r ./src ./test -lll -x .deployments/layer/dependencies1,.deployments/layer/dependencies2)

## Run the black code check
run-black:
	$(call execute_in_env, black ./src ./test --exclude '/deployments/layer/dependencies(1|2)')

## Run the flake8 linting check
run-flake8:

	$(call execute_in_env, flake8 ./src ./test --exclude=deployments/layer/dependencies1,deployments/layer/dependencies2 --max-line-length=88 --extend-ignore=E501)

## Run the mypy static type checks
run-mypy:
	$(call execute_in_env, mypy --explicit-package-bases .)
	
## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v --ignore=deployments/layer/dependencies1 --ignore=deployments/layer/dependencies2)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src test/ --cov-fail-under=80 --ignore=deployments/layer/dependencies1 --ignore=deployments/layer/dependencies2)

	

## Run all checks
run-checks: security-test run-black run-mypy unit-test run-flake8 check-coverage
