.binance.BASEURL:"https://data.binance.vision/data/spot/monthly/klines/"

.binance.hdb_dir:{
  $[.qi.isproc;
    .qi.path(.conf.DATA;.proc.self.stackname;`hdb;.proc.self.options`hdb);
    .qi.path .conf.BINANCE_HDB] /TODO
  }

/ Parse raw CSV lines into a typed table for one symbol
.binance.parse:{[sym;lines]
  c:("JFFFFF FJ";",")0:lines;
  n:count c 0;
  ms:c[0] div $[first[c 0]>1e16;1000000;first[c 0]>1e13;1000;1];  / ns->ms, us->ms, or ms
  times:1970.01.01D+1000000*ms;     / ms epoch -> q timestamp
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
  @[system;"curl -L -s --max-time 120 -o ",.qi.ospath[zip]," ",url;{[u;e].qi.error"Failed to fetch ",u,": ",e}[url;]];
  $[.qi.WIN;
    system"powershell -NoProfile -Command \"Expand-Archive -Path '",.qi.ospath[zip],"' -DestinationPath '",.qi.ospath[tmp],"' -Force\"";
    [lines:system"unzip -p ",.qi.spath zip;fp:lines]];
  data:.binance.parse[sym;fp];
  .qi.deldir tmp;
  data
  }

/ Check if sym already exists in a partition date
.binance.symexists:{[hdbpath;interval;date;sym]
  p:.qi.path(hdbpath;date;"BinanceKline",string interval;`sym);
  s:.qi.path(hdbpath;`sym);
  if[not .qi.exists p;:0b];
  sym in distinct get[s]get p
  }

/ Write one day's rows to HDB partition
.binance.writepart:{[hdbpath;interval;date;tbl]
  tname:`$"BinanceKline",string interval;
  .qi.os.ensuredir .qi.path(hdbpath;`$string date);
  partpath:.qi.path(hdbpath;`$string date;tname);
  .[.qi.path(partpath;`);();,;.Q.en[hdbpath;tbl]];
  .qi.info string[date]," ",string[count tbl]," rows";
  }


/ Backfill month by month, returns dates written
.binance.backfillsym:{[sym;start;end;interval;hdbpath]
  .qi.info"Backfilling ",string[sym]," ",string[interval]," ",string[start]," to ",string end;
  raze{[sym;interval;hdbpath;start;end;ym]
    / all dates in this month
    alldts:("d"$ym)+til("d"$ym+1)-"d"$ym;
    / skip fetch if sym already present for every date in the full month
    if[all .binance.symexists[hdbpath;interval;;sym] each alldts;
      .qi.info"Skipping ",string[sym]," ",string[ym],": already backfilled";
      :alldts where alldts within(start;end)];
    tbl:.binance.fetchmonth[sym;interval;ym];
    if[not count tbl;:`date$()];
    dts:distinct(`date$tbl`time)except 0Nd;
    / write all dates from zip where sym is missing (not just [start,end])
    dts:dts except dts where .binance.symexists[hdbpath;interval;;sym] each dts;
    {[hdbpath;interval;tbl;dt].binance.writepart[hdbpath;interval;dt;select from tbl where(`date$time)=dt]
      }[hdbpath;interval;tbl;] each dts;
    / return only dates within requested range
    dts where dts within(start;end)
    }[sym;interval;hdbpath;start;end;] each distinct`month$start+til 1+end-start;
  }

.binance.backfill:{[syms;start;end;interval;hdbpath]
  p:.qi.path hdbpath;
  dates:distinct raze .binance.backfillsym[;start;end;interval;p] each syms;
  tname:`$"BinanceKline",string interval;
  {[p;tname;y]t:.qi.path(p;y;tname);if[.qi.exists t;`sym xasc t;@[t;`sym;`p#]]}[p;tname;]each key[p] where key[p] like"[0-9]*";
  .Q.chk p;
  if[.qi.isproc;
    $[null h:.ipc.conn hdb:.qi.tosym .proc.self.options`hdb;
      .qi.info"Could not connect to ",string[hdb]," to initiate reload";
      [.qi.info"Initiating reload on ",string hdb;h"reload[]"]]];
  .qi.info"Backfill complete";
  tname
  }
