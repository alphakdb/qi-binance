.binance.BASEURL:"https://data.binance.vision/data/spot/monthly/klines/"

/ Parse raw CSV lines into a typed table for one symbol
.binance.parse:{[sym;lines]
  c:("JFFFFF FJ";",")0:lines;  
  n:count c 0;
  times:1970.01.01D+1000000*c 0;     / ms epoch -> q timestamp
  flip`time`sym`open`high`low`close`vwap`volume`feedtime`tptime!(times;n#sym;c 1;c 2;c 3;c 4;c[6]%c 5;c 5;n#.z.p;n#0Np)
  }

/ Download and parse one monthly zip, returns table
.binance.fetchmonth:{[sym;interval;ym]
  fname:("-"sv(string sym;string interval;string`int$`year$ym;-2#"0",string`mm$ym)),".zip";
  url:.binance.BASEURL,string[sym],"/",string[interval],"/",fname;
  .qi.info"Fetching ",url;
  tmp:.qi.local`tmp;
  .qi.os.ensuredir tmp;
  zip:.qi.path(tmp;`$fname);
  fp:.qi.path(tmp;`$(-4_fname),".csv");
  .qi.fetch[url;zip];
  $[.qi.WIN;
    system"powershell -NoProfile -Command \"Expand-Archive -Path '",.qi.ospath[zip],"' -DestinationPath '",.qi.ospath[tmp],"' -Force\"";
    [lines:system"unzip -p ",.qi.spath zip;fp:lines]];
  data:.binance.parse[sym;fp];
  $[.qi.WIN;hdel each(zip;fp);hdel zip];
  data
  }

/ Write one day's rows to HDB partition
.binance.writepart:{[hdbpath;date;tbl]
  .qi.os.ensuredir .qi.path(hdbpath;`$string date);
  partpath:.qi.path(hdbpath;`$string date;`BinanceKline1m);
  .[.qi.path(partpath;`);();,;.Q.en[hdbpath;tbl]];
  .qi.info string[date]," ",string[count tbl]," rows";
  }


/ Backfill month by month
.binance.backfillsym:{[sym;start;end;interval;hdbpath]
  .qi.info"Backfilling ",string[sym]," ",string[interval]," ",string[start]," to ",string end;
  {[sym;interval;hdbpath;ym]
    tbl:.binance.fetchmonth[sym;interval;ym];
    if[not count tbl;:()];
    {[hdbpath;tbl;dt].binance.writepart[hdbpath;dt;select from tbl where(`date$time)=dt]
      }[hdbpath;tbl;] each distinct`date$tbl`time
    }[sym;interval;hdbpath;] each distinct`month$start+til 1+end-start;
  }

.binance.backfill:{[syms;start;end;interval;hdbpath]
  p:.qi.path hdbpath;
  .binance.backfillsym[;start;end;interval;p] each syms;
  {t:.qi.path(x;y;`BinanceKline1m);`sym xasc t;@[t;`sym;`p#]}[p;]each key[p] where key[p] like"[0-9]*";
  .qi.info"Backfill complete";
  }