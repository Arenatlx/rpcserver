package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "macbook"
	password = "612713tlx"
	dbname   = "postgres"
)


type RequestPG struct{
	SQL string
	Count int
}

type ResultPG struct{
	Count int
	Result string
}

type PGX struct{

}

func (p *PGX)Require(args *RequestPG, reply *ResultPG)error{
	log.Println(args.Count)
	log.Println(args.SQL)
	ans, count := requestpg(args)
	reply.Count = count
	reply.Result = ans
	log.Println(*reply)
	return nil
}

func main() {
	/*
	pg := RequestPG{"select * from remotepg;",3}
	str, count := requestpg(&pg)
	fmt.Println(str, count)
	*/


	pgx := new(PGX)
	rpc.Register(pgx)
	rpc.HandleHTTP()
	listener,e := net.Listen("tcp","0.0.0.0:5434")
	if e!=nil {
		log.Println(e)
	}
	http.Serve(listener,nil)

}

func requestpg(pg *RequestPG)(ans string, count int){

	/*test go connect postgresql*/
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	//res,err:= db.Exec("select * from remotepg;")
	res, err:= db.Query(pg.SQL)
	if err!=nil{
		panic(err)
	}
	var temp = make([]interface{},pg.Count)
	var row = make([]string, pg.Count)
	for i:=0; i<pg.Count; i++{    //interface不是取址的，所以强制转化成string类型的地址
		temp[i] = &row[i]
	}
	ans = ""
	count = 0
	for res.Next(){
		err:= res.Scan(temp...)   //scan这边又只能传interface
		if err!= nil{
			fmt.Println(err)
		}else{
			//fmt.Println(row)
		}
		tempstr := strings.Join(row,"#")
		if ans!=""&&tempstr!=""{
			ans+=","
			ans+=tempstr
		}
		ans+=tempstr
		count++
	}
	return
	//log.Println()
	//fmt.Println("Successfully connected!")
}


