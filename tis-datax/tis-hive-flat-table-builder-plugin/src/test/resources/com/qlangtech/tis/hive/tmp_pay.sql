SELECT
  aa.totalpay_id
, concat_ws(';', collect_list(aa.aakindpay)) kindpay
, sum(aa.fee) fee
, (CASE WHEN (sum(aa.is_enterprise_card_pay_inner) > 0) THEN 1 ELSE 0 END) is_enterprise_card_pay
, concat_ws(',', collect_list(aa.pay_customer_ids)) pay_customer_ids
, aa.pt,aa.pmod
FROM
  (
   SELECT
     p.totalpay_id
   , sum(p.fee) fee
   , concat_ws('_', COALESCE(p.kindpay_id, '0'), CAST(count(1) AS string), CAST(round(sum(p.fee), 2) AS string), CAST(sum(((COALESCE(p.coupon_fee, 0) - COALESCE(p.coupon_cost, 0)) * COALESCE(p.coupon_num, 0))) AS string), COALESCE(CAST(min(p.pay_id) AS string), '0')) aakindpay
   , sum((CASE WHEN (p.type = 103) THEN 1 ELSE 0 END)) is_enterprise_card_pay_inner
   , concat_ws(',', collect_list(get_json_object(p.ext, '$.customerRegisterId'))) pay_customer_ids
   , p.pt,p.pmod
   FROM
     "order"."payinfo" as p
   WHERE ((length(p.kindpay_id) > 0) AND (p.is_valid = 1)) AND p.pt='20200603135447'
   GROUP BY p.totalpay_id, p.kindpay_id,p.pt,p.pmod
)  aa
GROUP BY aa.totalpay_id,aa.pt,aa.pmod
