# Cassandra-Advanced-Tools
This tool is designed to bring some extra features that are not in Cassandra or in DSE solutions.

- List all Kespaces on Cassandra 
- Audit a KeySpace: listing all keyspace tables. For each one, we have all fields with there types, and table's parameters.
- Stats: Listing a bench of keyspace stats.

## Quick view of parameters usage:

> java -jar CassandraTools.jar

![enter image description here](https://3.bp.blogspot.com/-A0BjGa661xs/XDi7obhQ-WI/AAAAAAABT54/VJau5RqrcEIXmbKq6miLj65UJSDqHPEzACLcBGAs/s640/Capture+d%25E2%2580%2599e%25CC%2581cran+2019-01-11+a%25CC%2580+16.44.30.png)

## Keyspaces listing

> java -jar CassandraTools.jar -h 127.0.0.1 -u toto -P toto -a

![enter image description here](https://4.bp.blogspot.com/-GGx_aL5JZio/XDi7shSAihI/AAAAAAABT58/hmmH1pG3oNotaJP71EDTQ51DjlQMSoqqwCLcBGAs/s640/Capture+d%25E2%2580%2599e%25CC%2581cran+2019-01-11+a%25CC%2580+16.46.54.png)


## Print Keyspace Audit on screen

> java -jar CassandraTools.jar -h 127.0.0.1 -u toto -P toto -a -k killr_video

![enter image description here](https://4.bp.blogspot.com/-uF26btsRwfk/XDi7u4MpV3I/AAAAAAABT6E/NvfXguz8S0UDuI7JplkW-ZcdZd9p0RTLgCLcBGAs/s640/Capture+d%25E2%2580%2599e%25CC%2581cran+2019-01-11+a%25CC%2580+16.47.42.png)

## Generate Keyspace Audit on HTML file

> java -jar CassandraTools.jar -h 127.0.0.1 -u toto -P toto -a -k killr_video -o killr_video.html

![enter image description here](https://2.bp.blogspot.com/-Ontzkpfb4hs/XDjD6Wz38wI/AAAAAAABT68/e-yAOfka-Bckp_wWD2ZnTrDk9rPsmQ71gCLcBGAs/s640/Capture+d%25E2%2580%2599e%25CC%2581cran+2019-01-11+a%25CC%2580+17.26.40.png)

## Generate Keyspace Audit on Tex file

> java -jar CassandraTools.jar -h 127.0.0.1 -u toto -P toto -a -t -k killr_video -o killr_video.tex

![enter image description here](https://2.bp.blogspot.com/-Lijtg6kCpkA/XDjD50DU12I/AAAAAAABT64/qgkG759MfNUnPGiY3YP0dXZwMgLJ1ooFgCLcBGAs/s640/Capture+d%25E2%2580%2599e%25CC%2581cran+2019-01-11+a%25CC%2580+17.18.22.png)

