CREATE EXTERNAL TABLE pet
  (name STRING, owner STRING, species STRING, sex STRING, birth STRING, death STRING)
  STORED BY 'com.aliyun.openservices.tablestore.hive.TableStoreStorageHandler'
  TBLPROPERTIES (
    "tablestore.endpoint"="YourEndpoint",
    "tablestore.access_key_id"="YourAccessKeyId",
    "tablestore.access_key_secret"="YourAccessKeySecret",
    "tablestore.table.name"="pet");
INSERT INTO pet VALUES("Fluffy", "Harold", "cat", "f", "1993-02-04", null),
       ("Claws", "Gwen", "cat", "m", "1994-03-17", null),
       ("Buffy", "Harold", "dog", "f", "1989-05-13", null),
       ("Fang", "Benny", "dog", "m", "1990-08-27", null),
       ("Bowser", "Diane", "dog", "m", "1979-08-31", "1995-07-29"),
       ("Chirpy", "Gwen", "bird", "f", "1998-09-11", null),
       ("Whistler", "Gwen", "bird", null, "1997-12-09", null),
       ("Slim", "Benny", "snake", "m", "1996-04-29", null),
       ("Puffball", "Diane", "hamster", "f", "1999-03-30", null);
SELECT * FROM pet;
SELECT * FROM pet WHERE birth > "1995-01-01";
SELECT count(*) FROM pet;
