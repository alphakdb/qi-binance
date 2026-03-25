.qi.import`ipc;
.qi.frompkg[`binance;`norm]
.qi.frompkg[`proc;`feed]
.qi.frompkg[`binance;`backfill] / .binance.backfill[`BTCUSDT`ETHUSDT;2024.01.10;2024.03.01;`1m;`:/home/iwickham/qihome/hdb]


tickers:"/"sv("," vs .conf.BINANCE_TICKERS),\:.conf.BINANCE_DATA
path:"/stream?streams=",tickers;
header:"GET ",path," HTTP/1.1\r\nHost: stream.binance.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
URL:`:wss://stream.binance.com:443

TD:01b!`BinanceKline2s`BinanceKline1m

msg.data:{[k] .feed.upd[TD k`x;norm.kline k]}

.z.ws:{
    data:$[`data in key d:.j.k[x]`data;d`data;d];
    if[data[`e]~"kline";msg.data data`k]
    }

start::{.feed.start[header;URL]}
