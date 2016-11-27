# warp10-scala
Warp10 driver for Scala, based on HTTP interface

```sh
$ docker run --volume=/path/to/docker/warp10:/data -p 8080:8080 -p 8081:8081 -d -i warp10io/warp10:1.0.16
$ docker ps
$ docker exec -t -i <dockerid> worf.sh
> encodeToken
> write
> <enter>
> <enter>
> <enter>
> 31536000000 # 1 year
> generate
> encodeToken
> read
> <enter>
> <enter>
> <enter>
> 31536000000 # 1 year
> generate
```