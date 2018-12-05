package main

import(
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	//"github.com/garyburd/redigo/redis"
	"github.com/go-redis/redis"
)

type RequestRedis struct{
	/*
	 * redis查询支持单点查询和范围查询
	 * 目前是现在是读上来，使用string的形式
	 * col offset 可以认为是有key， val， <key,val>,也就是两列
	*/
	QueryType int
	Offsets []int
	Value string
	Count int
}

type ResultRedis struct {
	Result string //同样用pg和csv的形式
}

type RedisX struct {

}

func (rx *RedisX)Require(args *RequestRedis,  reply *ResultRedis)error{
	log.Println("get a redis request here")
	log.Println(args)
	reply.Result = readRedis(args)
	log.Println(reply)
	return nil
}

func readRedis(rq *RequestRedis)string{
	client := redis.NewClient(&redis.Options{
		Addr:"localhost:6379",
		Password:"",
		DB:0,
	})
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

	resstr := ""
	if rq.QueryType==1{     //点查询
		val, err := client.Get(rq.Value).Result()
		if err != nil {
			panic(err)
		}
		for i:=0; i<len(rq.Offsets); i++{
			if rq.Offsets[i]==0{   //说明想要key
				if resstr!=""{
					resstr+="#"
					resstr+=rq.Value
				}else{
					resstr+=rq.Value
				}
			}
			if rq.Offsets[i]==1{   //说明想要val
				if resstr!=""{
					resstr+="#"
					resstr+=val
				}else{
					resstr+=val
				}
			}
		}
	}else if rq.QueryType==2{
		vals,err := client.Keys(rq.Value).Result()
		if err!=nil{
			panic(err)
		}
		/*
		 *  得到key之后再对每个key进行点查询
		*/
		for i, key :=range vals{
			val, err:=client.Get(key).Result()
			if err!=nil{
				panic(err)
			}
			if i>0{
				resstr+=","
			}
			oneval:=""
			for i:=0; i<len(rq.Offsets); i++{
				if rq.Offsets[i]==0{   //说明想要key
					if oneval!=""{
						oneval+="#"
						oneval+=key
					}else{
						oneval+=key
					}
				}
				if rq.Offsets[i]==1{   //说明想要值
					if oneval!=""{
						oneval+="#"
						oneval+=val
					}else{
						oneval+=val
					}
				}
			}
			resstr+=oneval

		}
	}else if rq.QueryType==4{
		vals,err := client.Keys("*").Result() //全部查询
		if err!=nil{
			panic(err)
		}
		/*
		 *  得到key之后再对每个key进行点查询
		*/
		for i, key :=range vals {
			val, err := client.Get(key).Result()
			if err != nil {
				panic(err)
			}
			if i > 0 {
				resstr += ","
			}
			oneval := ""
			for i := 0; i < len(rq.Offsets); i++ {
				if rq.Offsets[i] == 0 { //说明想要key
					if oneval != "" {
						oneval += "#"
						oneval += key
					}else{
						oneval += key
					}
				}
				if rq.Offsets[i] == 1 { //说明想要值
					if oneval != "" {
						oneval += "#"
						oneval += val
					}else{
						oneval += val
					}
				}
			}
			resstr += oneval
		}
	}
	return resstr

}

func main() {
	redisx := new(RedisX)
	rpc.Register(redisx)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "0.0.0.0:5435")
	if e!=nil{
		panic(e)
	}
	http.Serve(l, nil)
}
