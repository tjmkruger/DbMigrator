# Db Updater

Used to update sql databases by applying transitions. 

## Requirements:

1. Be in source control
2. Run test updates on databases
3. Use with continuous integration
4. Everyone works on their own database
5. Dogfooding updates

## Steps include:

1. Migrating table schema
2. Drop all views, stored proc and functions
3. Recreate all views, stored procs and functions

More reading:
	DbUp: http://dbup.github.io/
	Paul Stovell Post: http://paulstovell.com/blog/database-deployment

