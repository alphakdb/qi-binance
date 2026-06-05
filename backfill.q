.binance.BASEURL:"https://data.binance.vision/data/spot/monthly/klines/"
.binance.DAILYURL:"https://data.binance.vision/data/spot/daily/klines/"

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

/ Download and parse one daily zip, returns table
.binance.fetchday:{[sym;interval;dt]
  ds:"-"sv"."vs string dt;
  fname:("-"sv(string sym;string interval;ds)),".zip";
  url:.binance.DAILYURL,string[sym],"/",string[interval],"/",fname;
  .qi.info"Fetching ",url;
  tmp:.qi.local`tmp;
  .qi.os.ensuredir tmp;
  zip:.qi.path(tmp;`$fname);
  @[system;"curl -L -s --max-time 60 -o ",.qi.ospath[zip]," ",url;{[u;e].qi.error"Failed to fetch ",u,": ",e}[url;]];
  $[.qi.WIN;
    [system"powershell -NoProfile -Command \"Expand-Archive -Path '",.qi.ospath[zip],"' -DestinationPath '",.qi.ospath[tmp],"' -Force\"";
     lines:get .qi.path(tmp;`$(-4_fname),".csv")];
    lines:system"unzip -p ",.qi.spath zip];
  data:.binance.parse[sym;lines];
  .qi.deldir tmp;
  data
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

/ Persistent index of completed (sym;interval;date) — O(1) skip check
.binance.IDXFILE:`binance_backfilled;

.binance.rebuildidx:{[hdbpath]
  / 1. Force hdbpath to be a single hsym atom
  hdbpath:hsym .qi.tosym hdbpath;
  .qi.info"Building binance backfill index from HDB (one-time)...";
  
  empty:flip`sym`interval`date!"ssd"$\:();
  idxFile:.qi.path(hdbpath;.binance.IDXFILE);
  
  / 2. Ensure the directory actually exists on disk
  if[not .qi.exists hdbpath;.qi.os.ensuredir hdbpath];

  s:.qi.path(hdbpath;`sym);
  / If no sym file, it's a fresh HDB
  if[not .qi.exists s;idxFile set empty;:empty];
  
  symenum:get s;
  k:key hdbpath;
  dparts:k where k like "[0-9]*";
  
  / 3. If no partitions found, save empty index and exit
  if[not count dparts;idxFile set empty;:empty];

  rows:raze{[hdbpath;symenum;dt]
    targetDir:.qi.path(hdbpath;dt);
    tnames:k1 where(k1:key targetDir) like "BinanceKline*";
    raze{[hdbpath;symenum;dt;tname]
      p:.qi.path(hdbpath;dt;tname;`sym);
      if[not .qi.exists p;:()];
      syms:distinct symenum get p;
      if[not count syms;:()];
      ([]sym:syms;interval:count[syms]#`$12_string tname;date:count[syms]#`date$string dt)
    }[hdbpath;symenum;dt;] each tnames
  }[hdbpath;symenum;] each dparts;
  
  idx:$[count rows;rows;empty];
  idxFile set idx;
  .qi.info"Index built: ",string[count idx]," entries";
  idx
 }

.binance.loadidx:{[hdbpath]
  p:.qi.path(hdbpath;.binance.IDXFILE);
  $[.qi.exists p;get p;.binance.rebuildidx hdbpath]
  }

/ Write one day's rows to HDB partition
.binance.writepart:{[hdbpath;interval;date;tbl]
  tname:`$"BinanceKline",string interval;
  .qi.os.ensuredir .qi.path(hdbpath;`$string date);
  partpath:.qi.path(hdbpath;`$string date;tname);
  .[.qi.path(partpath;`);();,;.Q.en[hdbpath;tbl]];
  .qi.info string[date]," ",string[count tbl]," rows";
  }

.binance.backfillsym:{[s;start;end;int;hdbpath]
  .qi.info"Backfilling ",string[s]," ",string[int]," ",string[start]," to ",string end;
  
  / Load index and filter using renamed variables to avoid column shadowing
  .binance.IDX:.binance.loadidx hdbpath;
  donedts:exec date from .binance.IDX where sym=s,interval=int;
  
  / Calculate missing months
  all_dts:start+til 1+"i"$end-start;
  missingmos:distinct `month$ all_dts except donedts;
  
  .qi.info"Found ",string[count donedts]," existing days. Months to fetch: ",string count missingmos;

  if[not count missingmos;
    .qi.info"Already fully backfilled";
    :donedts where donedts within (start;end)
  ];

  raze {[s;int;hdbpath;start;end;donedts;ym]
    / Use daily zips for the current (incomplete) month, monthly zip otherwise
    tbl:$[ym=`month$.z.d;
      [startdt:`date$ym;enddt:`date$ym+1;daydts:startdt+til`long$enddt-startdt;
       daydts:daydts where (daydts within(start;end))&daydts<.z.d;
       daydts:daydts except donedts;
       if[not count daydts;:`date$()];
       raze .binance.fetchday[s;int;] each daydts];
      .binance.fetchmonth[s;int;ym]];
    if[not count tbl;:`date$()];

    / Filter dates in this month not already in the index for this sym+int
    dts:(distinct[`date$tbl`time] except 0Nd) except donedts;

    / Write partitions to disk
    {[hdbpath;int;tbl;dt]
      .binance.writepart[hdbpath;int;dt;select from tbl where (`date$time)=dt]
    }[hdbpath;int;tbl;] each dts;

    / Update index and PERSIST (with path resolution safety)
    if[count dts;
      .binance.IDX,:([]sym:count[dts]#s;interval:count[dts]#int;date:dts);
      (.qi.path(hdbpath;.binance.IDXFILE)) set .binance.IDX
    ];

    dts where dts within (start;end)
  }[s;int;hdbpath;start;end;donedts;] each missingmos
 }

.binance.backfill:{[syms;start;end;interval;hdbpath]
  p:.qi.path hdbpath;
  dates:distinct raze .binance.backfillsym[;start;end;interval;p] each syms;
  tname:`$"BinanceKline",.qi.tostr interval;
  if[count dates;
    {[p;tname;y]t:.qi.path(p;y;tname);if[.qi.exists t;`sym xasc t;@[t;`sym;`p#]]}[p;tname;]each`$string dates;
    .Q.chk p];
  if[.qi.isproc;
    $[null h:.ipc.conn hdb:.qi.tosym .proc.self.options`hdb;
      .qi.info"Could not connect to ",string[hdb]," to initiate reload";
      [.qi.info"Initiating reload on ",string hdb;h"reload[]"]]];
  .qi.info"Backfill complete";
  tname
  }
