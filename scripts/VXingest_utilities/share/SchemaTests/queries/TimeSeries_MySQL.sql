select
   ceil(3600*floor(((m0.time) + 3600 / 2) / 3600)) as avtime,
   count(distinct ceil(3600*floor((m0.time + 1800) / 3600))) as N_times,
   min(ceil(3600*floor((m0.time + 1800) / 3600))) as min_secs,
   max(ceil(3600*floor((m0.time + 1800) / 3600))) as max_secs,
   sum(if((m0.ceil < 300) 
   and 
   (
      o.ceil < 300
   )
, 1, 0)) as hit,
   sum(if((m0.ceil < 300) 
   and NOT (o.ceil < 300), 1, 0)) as fa,
   sum(if(NOT (m0.ceil < 300) 
   and 
   (
      o.ceil < 300
   )
, 1, 0)) as miss,
   sum(if(NOT (m0.ceil < 300) 
   and NOT (o.ceil < 300), 1, 0)) as cn,
   group_concat(ceil(3600*floor((m0.time + 1800) / 3600)), ';', if((m0.ceil < 300) 
   and 
   (
      o.ceil < 300
   )
, 1, 0), ';', if((m0.ceil < 300) 
   and NOT (o.ceil < 300), 1, 0), ';', if(NOT (m0.ceil < 300) 
   and 
   (
      o.ceil < 300
   )
, 1, 0), ';', if(NOT (m0.ceil < 300) 
   and NOT (o.ceil < 300), 1, 0) 
order by
   ceil(3600*floor((m0.time + 1800) / 3600))) as sub_data,
   count(m0.ceil) as N0 
from
   ceiling2.obs as o,
   ceiling2.HRRR_OPS as m0 
where
   1 = 1 
   and m0.madis_id = o.madis_id 
   and m0.time = o.time 
   and m0.madis_id in
   (
      '2461',
      '2464',
      '148072',
      '38162',
      '2465',
      '2529',
      '2479'
   )
   and m0.time >= 1664236800 - 900 
   and m0.time <= 1664841600 + 900 
   and o.time >= 1664236800 - 900 
   and o.time <= 1664841600 + 900 
   and m0.fcst_len = 6 
group by
   avtime 
order by
   avtime;