# DSCI551 Final Project

## `How to run Relational Backend (Note that you need to add ; after each query)`
- Relational
  - To start our Backend Database System:
   - python3 main.py

  - you will see: relational or nosql in the terminal

  - Let test relational database first: relational 

  - Must Load data first: 
    - load data from movies.csv;                                  
    - Note that you need to add ; after each query. This command line will divided our movies.csv into chunks and you can find all the chunks under Storage/Realtional/movies.

  - let test insert command line:
    - insert into movies with data name=Titanic,genre=Romantic;      
    - This command line will insert one row in the last chunk, if you go to Storage/Realtional/movies, and open the last chunk_xxx.csv, you will find the inserted data. If the last chunk is full, it will direcly created a new chunk and insert data into this new chunk.


  - let test update command line
     - update in movies where name=Titanic and set name=Titanic2,runtime=120;   (You can see updated data after excuting.)


  - let test delete command line:
     - delete from movies where name=Titanic2;

  - let test projection command line
     - show field name from movies;               (This will select one column)
     - show field name,rating from movies;        (This will select multiple columns)

  - let test filtering:
     - show data name,rating from movies where rating=R;

  - let test grouping + aggreagtion:
    - find max(runtime) in movies group by genre;
    - find min(runtime) in movies group by name;
    - find count(name) in movies group by genre;
    - find avg(runtime) in movies group by genre;
    - find sum(year) in movies group by name;
  
  
  - let test aggregarion ONLY: 
    - find max(year) in movies;
    - find min(year) in movies;
    - find count(runtime) in movies;


  - let test grouping ONLY:
    - group movies by genre;


  - let test join


  - let test ordering:
    - sort data in movies by genre asc;              (this will sort genre in ascending order)
    - sort data in movies by year desc;              (this will sort year in descending order)


  - lest test show table command:
    - show tables;         


  - lest test drop table command:
    - drop table movies;            (you can notice that movies table is dropped after this command)


  - let test create table function, the command line is "create table xxx(id,name,....);" 
    - create table student(id,name); 
    - After you excute: create table student(id,name); in the terminal, you will see there is a student.txt created in Storage/Relational/student, if you excute inserting coomand later, chunks will be created and data will be inserted.


## `How to run NoSQL Backend (Note that you need to add ; after each query)`

- NoSQL:
  - Re-start our Backend Database System:
    - python3 main.py

  - choose nosql database:
    - nosql

  - Must load data first:
    - load data from rotten_tomatoes_movies.csv;
    - This command line will switch rotten_tomatoes_movies.csv to json format and divided into chunks. you can find all the chunks under Storage/NoSQL/rotten_tomatoes_movies. 

  - let test insert command line:
    - insert into rotten_tomatoes_movies with data movie_title=Titanic,runtime=120; 
    - after excuting thie command line, you will see new data insert into the last chunk under Storage/NoSQL/rotten_tomatoes_movies.

  - let test update command line:
    - update in rotten_tomatoes_movies where movie_title=Titanic and set movie_title=Titanic2,directors=James; 


  - let test delete command line:
    - delete from rotten_tomatoes_movies where movie_title=Titanic2;

  
  - let test projection:
     - show field movie_title from rotten_tomatoes_movies;               (This will select one column)
     - show field movie_title,directors from rotten_tomatoes_movies;     (This will select multiple columns)

  
  - let test filtering:
    - show data movie_title,runtime from rotten_tomatoes_movies where runtime=120;

  
  - let test grouping + aggreagtion:
    - find max(runtime) in rotten_tomatoes_movies group by genres;
    - find min(runtime) in rotten_tomatoes_movies group by genres;
    - find count(movie_title) in rotten_tomatoes_movies group by genres;
    - find avg(runtime) in rotten_tomatoes_movies group by genres;
    - find sum(runtime) in rotten_tomatoes_movies group by movie_title;


  - let test aggregarion ONLY: 
    - find max(runtime) in rotten_tomatoes_movies;
    - find avg(runtime) in rotten_tomatoes_movies;
    - find count(runtime) in rotten_tomatoes_movies;


  - let test grouping ONLY:
    - group rotten_tomatoes_movies by genres;

  
  - let test join:



  - let test ordering:
    - sort data in rotten_tomatoes_movies by genres asc;              (this will sort genre in ascending order)
    - sort data in rotten_tomatoes_movies by genres desc;              (this will sort genre in descending order)


  - test create table command:
    - create table students(id,name,year);              it will directly create a table under Storage/NoSQL
    - insert into student with data id=1,name=John;     it will create chunk under Storage/NoSQL/student



  - lest test show table command:
    - show tables;         


  - lest test drop table command:
    - drop table rotten_tomatoes_movies;            (you can notice that rotten_tomatoes_movies table is dropped after this command)
 
 


## `How to run Web`
- To start our frontend
  - python3 run.py        
  - Note that you may need to install flask 


- Selete Relational or NoSQL on Web
  - let selet relational first


- Upload dataset:
  - if you want to test your own dataset, you can click choose file button from your local machine and click upload button to upload it (Note that file should be CSV), the upload button is do the same thing as our load_data in our backend. It will store your uploaded file into /ToBeLoaded, and divided the file into chunk and store them under /Storage/Relational.
  - For convenience, we still test movies.csv. When testing the database, you already loaded movies.csv and it was chunked. So you don't need to upload it again, but if you want, you can upload it again


- Projection:
  - Table Name: movies
  - Field(s): name,rating
  - Note that Make sure there are no Spaces before or after each text
  - after click "Show Data" button, scroll down and You can see all projection data in the Result Display area at the bottom of the screen



- filtering:
  - Table Name: movies
  - Fields: name,rating
  - Condition: rating=R


- For convenience, test Insertion first:
  - Table Name: movies
  - Data: name=Titanic3
  - you can see new data inserted into last chunk in our backend, and our front end show "insertion succeeded"


- Updating:
  - Table Name: movies
  - Condition: name=Titanic3 
  - Data: name=Titanic4
  - you can see updated data in last chunk in our backend, and our front end show "update succeeded"


- Deletion
  - Table Name: movies
  - Condition: name=Titanic4


- Sorting
  - Table Name: movies
  - Field: genre
  - you can choose asc or not


- Join




- Aggregation + GroupBy(test max)
  - Table Name: movies
  - To Find: max(runtime)
  - Group By: genre

- Aggregation + GroupBy(test min)
  - Table Name: movies
  - To Find: min(runtime)
  - Group By: genre

- Aggregation + GroupBy(test avg)
  - Table Name: movies
  - To Find: avg(runtime)
  - Group By: genre


- Aggregation ONLY:
  - Table Name: movies
  - To Find: max(year)
  - leave Group By input empty 


- Grouping ONLY:
  - Table Name: movies
  - leave To find input empty
  - Group By: genre



## `How to run NoSQL on Web`

- Please select NoSQL button first

- Upload dataset:
  - if you want to test your own dataset, you can click choose file button from your local machine and click upload button to upload it (Note that file should be CSV), the upload button is do the same thing as our load_data in our backend. It will store your uploaded file into /ToBeLoaded, and switch them to JSON format and divided the file into chunk and store them under /Storage/NoSQL
  - For convenience, we still test rotten_tomatoes_movies.csv. When testing the database, you already loaded rotten_tomatoes_movies.csv and it was chunked. So you don't need to upload it again, but if you want, you can upload it again


- Projection:
  - Table Name: rotten_tomatoes_movies
  - Field(s): movie_title,genres


- filtering:
  - Table Name: rotten_tomatoes_movies
  - Fields: movie_title,runtime
  - Condition: runtime=120


- For convenience, test Insertion first:
  - Table Name: rotten_tomatoes_movies
  - Data: movie_title=Titanic3
  - you can see new data inserted into last chunk in our backend, and our front end show "insertion succeeded"


- Updating:
  - Table Name: rotten_tomatoes_movies
  - Condition: movie_title=Titanic3 
  - Data: movie_title=Titanic4
  - you can see updated data in last chunk in our backend, and our front end show "update succeeded"


- Deletion
  - Table Name: rotten_tomatoes_movies
  - Condition: movie_title=Titanic4


- Sorting
  - Table Name: rotten_tomatoes_movies
  - Field: genres
  - you can choose asc or not
  - Note that it will be a liitle slow since rotten_tomatoes_movies has too many chunks


- Join




- Aggregation + GroupBy(test max)
  - Table Name: rotten_tomatoes_movies
  - To Find: max(runtime)
  - Group By: genres


- Aggregation + GroupBy(test min)
  - Table Name: rotten_tomatoes_movies
  - To Find: min(runtime)
  - Group By: genres

- Aggregation + GroupBy(test avg)
  - Table Name: rotten_tomatoes_movies
  - To Find: avg(runtime)
  - Group By: genres


- Aggregation ONLY:
  - Table Name: rotten_tomatoes_movies
  - To Find: max(runtime)
  - leave Group By input empty 


- Grouping ONLY:
  - Table Name: rotten_tomatoes_movies
  - leave To find input empty
  - Group By: genres



















