# Parse Rentals

This is a small project that started with me having some problems with finding fair price housing near my work.

I created a fast simple webscraping tool that scraped the most common websites of rentals in Curitiba.

After this I realized that this was a common problem among friends of my age and I could create a product for helping people. 
Because I was curious of playing with the cloud, terraform and related I started developing this

## Segregation 

This is a simple folder schema mostly for automatically CI\CD for google cloud.

**functions** are the lambda cloud functions
* Parsing
* Pagination
* Downloading the HTML (storing data is cheap)

**jobs** will be spark jobs and related necessary for the process

**samples** is data for testing the functions

**tests** are automated tests that I'm creating

**code_cloud_unrelated** are python scripts for parsing using locally and fixing stupid things that I did

## Steps

My first ambtion here is to have workflow from the imoevelweb website

### Version 1.0 
1. Creating cloud functions with in a smart way (logging, error handling, as reusable as possible)
2. Creating a solution to bring the data from google cloud storage to bigquery
3. Create a user simple interface for searching using bigquery as backend (easier and cheaper then cloud SQL)
4. Create a automatically process that detects ulrs offline
5. Finding problems, treating bigquery data and others.

### This is a simple fun mock project because I love data, cloud and speed development, the main purpose is to help people with find better housing at Brasil.

#### Notes
The tagging system will be strange because that's what I'm using to CD into GCP.