#!/usr/bin/env bash
mkdir -p package
pip install -r requirements-runtime.txt --target package
cd package
zip -rA ../../../../preingest-importer .
cd ..
zip -rA ../../../preingest-importer lambda_function.py
cd ../../../
zip -rA preingest-importer common/