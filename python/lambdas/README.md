# Python Lambdas

Python lambdas need to be built and deployed differently than other lambdas (Scala, Kotlin etc). Once you've created your lambda

1. Create a directory with (optionally but recommended) the same name as it is/will be defined in the lambda terraform module but in kebab case
2. Add the Python lambda file
3. (Optional but recommended) Give the file the same name as it is/will be defined in the lambda terraform module but in snake case
4. In the build.yml file, in the "run" section of the "pre-deploy" job
   1. If it's a new Importer lambda
      * add this line `cp preingest-importer target/outputs/preingest-<type>-importer ` under the other similar commands
   2. If it's not an Importer lambda
      * add this line `zip -jA <lambda name in kebab case> python/lambdas/<lambda name in kebab case>/<lambda name in snake case>.py` under the other similar commands
      * and add this line `cp <lambda name in kebab case> target/outputs` under the other similar commands
5. Commit this file
