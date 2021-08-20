###############################################################################
#                                                                             #
#                                 IMPORTS                                     #
#                                                                             #
###############################################################################

print('--------------------------------------------')
print('Arranca IMPORTS')

from Carga_Descarga import *
from CRM_Central import *
import time

print('Finalizo IMPORTS')

###############################################################################
#                                                                             #
#                                CONSTANTES                                   #
#                                                                             #
###############################################################################

print('--------------------------------------------')
print('Arranca CONSTANTES')

# Fechas
today = datetime.date.today()
idx = (today.weekday() + 1) % 7
fin = today - datetime.timedelta(days=idx)
start = str(fin - datetime.timedelta(6*7-1))
start_tc = str(fin - datetime.timedelta(6*7-1) - relativedelta(months=12))
if today.day == 1:
    end = str(today - relativedelta(days=1))
    ftm = str(today - relativedelta(days=1))
    itm = str((today - relativedelta(days=1)).replace(day=1))
    flm = str((today - relativedelta(days=1)).replace(day=1) - relativedelta(days=1))
    ilm = str(((today - relativedelta(days=1)).replace(day=1) - relativedelta(days=1)).replace(day=1))
else:
    end = str(today + relativedelta(months=1) - relativedelta(days=(today + relativedelta(months=1)).day))
    itm = str(today.replace(day=1))
    ftm = str(today + relativedelta(months=1) - relativedelta(days=(today + relativedelta(months=1)).day))
    flm = str(today.replace(day=1) - relativedelta(days=1))
    ilm = str((today.replace(day=1) - relativedelta(days=1)).replace(day=1))

# Fechas Weekly
today = datetime.date.today()
idx = (today.weekday() + 1) % 7
fin = today - datetime.timedelta(days=idx)
inicio = fin - datetime.timedelta(1*7-1)
fin = str(fin)
inicio = str(inicio)

# Me traigo el rr
rr = func_rr()

print('Finalizo CONSTANTES')

###############################################################################
#                                                                             #
#                              DASHBOARD PAISES                               #
#                                                                             #
###############################################################################

print('--------------------------------------------')
print('Arranca DASHBOARD PAISES')

###########################################################
#
#       QUERIES
#
###########################################################

print('--------------------------------------------')
print('Arranca Queries')

# 60GB
q_tc = '''WITH coupons_table AS (
    SELECT cn.country_name AS country,
           IFNULL(c.city_name,'-') AS city,
           tc.talon_campaign_id AS campaign_id,
           tc.coupon_id AS coupon_id,
           o.registered_date AS fecha,
           COUNT(DISTINCT o.order_id) AS redeemed,
           COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.order_id ELSE NULL END) AS redeemed_confirmed,
           COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS acquisitions,
           --SUM(CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN tc.coupon_used_amount ELSE 0 END) AS used_amount,
           --SUM(CASE WHEN bi.payment_mode != 'TOTAL_AMOUNT' THEN tc.coupon_used_amount ELSE 0 END) AS used_amount_shopper,
           --SUM(CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS used_amount_usd,
           --SUM(CASE WHEN bi.payment_mode != 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS used_amount_usd_shopper,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' AND bi.payment_mode = 'TOTAL_AMOUNT' THEN tc.coupon_used_amount ELSE 0 END) AS used_amount_confirmed,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' AND bi.payment_mode != 'TOTAL_AMOUNT' THEN tc.coupon_used_amount ELSE 0 END) AS used_amount_confirmed_shopper,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' AND bi.payment_mode = 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS used_amount_confirmed_usd,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' AND bi.payment_mode != 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS used_amount_confirmed_usd_shopper
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
    INNER JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` AS tc ON o.order_id = tc.order_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON o.restaurant.id = p.partner_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_billing_info` AS bi ON p.billingInfo.billing_info_id = bi.billing_info_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON tc.coupon_country_id = cn.country_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce ON cn.currency_id = ce.currency_id AND DATE_TRUNC(o.registered_date,MONTH) = ce.currency_exchange_date
    LEFT JOIN `peya-argentina.user_santiago_curat.city_usuarios` AS u ON tc.user_id = u.user AND cn.country_name = u.country
    WHERE o.registered_date BETWEEN DATE_ADD(CURRENT_DATE(),INTERVAL -30 DAY) AND CURRENT_DATE()
          AND DATE(tc.coupon_created_at) >= DATE_ADD(CURRENT_DATE(),INTERVAL -395 DAY)
    GROUP BY 1,2,3,4,5),
    creation_table AS (
    SELECT cn.country_name AS country,
           IFNULL(u.city,'-') AS city,
           tc.talon_campaign_id AS campaign_id,
           COUNT(DISTINCT CASE WHEN tc.coupon_usage_limit = 1 THEN tc.coupon_id ELSE NULL END) AS created1,
           SUM(CASE WHEN tc.coupon_usage_limit > 1 THEN tc.coupon_usage_limit ELSE 0 END) AS created2
    FROM `peya-bi-tools-pro.il_growth.fact_talon_coupons` AS tc
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON tc.coupon_country_id = cn.country_id
    LEFT JOIN `peya-argentina.user_santiago_curat.city_usuarios` AS u ON tc.user_id = u.user AND cn.country_name = u.country
    WHERE DATE(tc.coupon_created_at) >= DATE_ADD(CURRENT_DATE(),INTERVAL -395 DAY)
          AND (DATE(tc.coupon_end) >= DATE_ADD(CURRENT_DATE(),INTERVAL -30 DAY) OR tc.coupon_start IS NULL)
    GROUP BY 1,2,3),
    coupons_final AS (
    SELECT ct.country AS country,
           ct.city AS city,
           ct.campaign_id AS campaign_id,
           SUM(ct.redeemed) AS redeemed,
           SUM(ct.redeemed_confirmed) AS redeemed_confirmed,
           SUM(ct.acquisitions) AS acq,
           --SUM(ct.used_amount) AS used_amount,
           --SUM(ct.used_amount_shopper) AS used_amount_shopper,
           --SUM(ct.used_amount_usd) AS used_amount_usd,
           --SUM(ct.used_amount_usd_shopper) AS used_amount_usd_shopper,
           SUM(ct.used_amount_confirmed) AS used_amount_confirmed,
           SUM(ct.used_amount_confirmed_shopper) AS used_amount_confirmed_shopper,
           SUM(ct.used_amount_confirmed_usd) AS used_amount_confirmed_usd,
           SUM(ct.used_amount_confirmed_usd_shopper) AS used_amount_confirmed_usd_shopper
    FROM coupons_table AS ct
    GROUP BY 1,2,3),
    creation_final AS (
    SELECT ct.country AS country,
           ct.city AS city,
           ct.campaign_id AS campaign_id,
           SUM(ct.created1) + SUM(ct.created2) AS created
    FROM creation_table AS ct
    GROUP BY 1,2,3)
SELECT IFNULL(crf.country,cf.country) AS Country,
       IFNULL(crf.city,cf.city) AS City,
       IFNULL(tca.talon_campaign_name,'-') AS Campaign,
       SUM(crf.created) AS Created,
       SUM(cf.redeemed) AS Redeemed,
       SUM(cf.redeemed_confirmed) AS Redeemed_Confirmed,
       SUM(cf.acq) AS Acq,
       --SUM(cf.used_amount) AS Used_Amount,
       --SUM(cf.used_amount_shopper) AS Used_Amount_Shopper,
       --SUM(cf.used_amount_usd) AS Used_Amount_USD,
       --SUM(cf.used_amount_usd_shopper) AS Used_Amount_USD_Shopper,
       SUM(cf.used_amount_confirmed) AS Used_Amount_Confirmed,
       SUM(cf.used_amount_confirmed_shopper) AS Used_Amount_Confirmed_Shopper,
       SUM(cf.used_amount_confirmed_usd) AS Used_Amount_Confirmed_USD,
       SUM(cf.used_amount_confirmed_usd_shopper) AS Used_Amount_Confirmed_USD_Shopper
FROM creation_final AS crf
FULL OUTER JOIN coupons_final AS cf ON crf.country = cf.country 
                                       AND crf.city = cf.city
                                       AND crf.campaign_id = cf.campaign_id
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` AS tca ON crf.campaign_id = tca.talon_campaign_id
GROUP BY 1,2,3'''

# 2GB
q_fl = '''SELECT DISTINCT cn.country_name AS Country,
       IFNULL(u.city,'-') AS City,
       tca.talon_campaign_name AS Campaign
FROM `peya-bi-tools-pro.il_growth.fact_talon_coupons` AS tc
LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON tc.coupon_country_id = cn.country_id
LEFT JOIN `peya-argentina.user_santiago_curat.city_usuarios` AS u ON tc.user_id = u.user AND cn.country_name = u.country
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` AS tca ON tc.talon_campaign_id = tca.talon_campaign_id
WHERE DATE(tc.coupon_created_at) >= DATE_ADD(CURRENT_DATE(),INTERVAL -3 DAY)'''

# 23GB
q_st = '''WITH coupons_table AS (
    SELECT tc.order_id AS order_id,
           tc.user_id AS user_id,
           tca.talon_campaign_name AS campaign,
           tc.coupon_used_amount AS amount_used
    FROM `peya-bi-tools-pro.il_growth.fact_talon_coupons` AS tc
    LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` AS tca ON tc.talon_campaign_id = tca.talon_campaign_id
    WHERE tc.order_id IS NOT NULL
          AND DATE(tc.order_registered_date) BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -4 MONTH),MONTH) AND CURRENT_DATE()),
    orders_table AS (
    SELECT o.user.id AS user,
           cn.country_name AS country,
           IFNULL(c.city_name,'-') AS city,
           o.order_status AS status,
           o.order_id AS order_id,
           o.registered_date AS fecha,
           CASE WHEN ct.campaign IS NULL THEN 0 ELSE 1 END AS crm,
           IFNULL(o.shipping_amount_no_discount + o.amount_no_discount,0) AS total_paid,
           SUM(od.discount_amount) AS total_disc,
           SUM(CASE WHEN UPPER(od.discount_type_name) = 'VOUCHER' THEN od.discount_amount ELSE 0 END) AS total_voucher_disc
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o,
    UNNEST(discounts) AS od
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
    LEFT JOIN coupons_table AS ct ON o.order_id = ct.order_id
    WHERE o.registered_date BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -4 MONTH),MONTH) AND CURRENT_DATE()
    GROUP BY 1,2,3,4,5,6,7,8),
    acq_table AS (
    SELECT ot.user AS user,
           ot.country AS country,
           ot.city AS city,
           ot.fecha AS fecha,
           ct.campaign AS campaign,
           SUM(ct.amount_used) AS amount_used
    FROM orders_table AS ot
    INNER JOIN coupons_table AS ct ON ot.order_id = ct.order_id
    GROUP BY 1,2,3,4,5),
    cohorts_table AS (
    SELECT acq.user AS user,
           acq.country AS country,
           acq.campaign AS campaign,
           acq.city AS city,
           acq.fecha AS fecha,
           acq.amount_used AS original_amount_used,
           CASE WHEN DATE_DIFF(ot.fecha,acq.fecha,DAY) = 0 THEN 'ORIGINALS'
                WHEN DATE_DIFF(ot.fecha,acq.fecha,DAY) BETWEEN 1 AND 30 THEN 'M1'
                WHEN DATE_DIFF(ot.fecha,acq.fecha,DAY) BETWEEN 31 AND 60 THEN 'M2'
                WHEN DATE_DIFF(ot.fecha,acq.fecha,DAY) BETWEEN 61 AND 90 THEN 'M3' END AS cohort,
           IFNULL(COUNT(DISTINCT CASE WHEN ot.status != 'CONFIRMED' THEN NULL ELSE ot.order_id END),0) AS orders,
           SUM(CASE WHEN ot.status != 'CONFIRMED' THEN 0 ELSE ot.total_paid END) AS total_paid,
           SUM(CASE WHEN ot.status != 'CONFIRMED' THEN 0 ELSE ot.total_disc END) AS total_disc,
           SUM(CASE WHEN ot.status != 'CONFIRMED' THEN 0 ELSE ot.total_voucher_disc END) AS total_voucher_disc
    FROM acq_table AS acq
    LEFT JOIN orders_table AS ot ON acq.user = ot.user
    WHERE ot.fecha BETWEEN acq.fecha AND CURRENT_DATE()
    GROUP BY 1,2,3,4,5,6,7)
SELECT FORMAT_DATE('%Y-%m', ct.fecha) AS Month,
       ct.country AS Country,
       ct.city AS City,
       ct.campaign AS Campaign,
       SUM(ct.original_amount_used) AS Original_Amount_Used,
       COUNT(DISTINCT CASE WHEN ct.cohort = 'ORIGINALS' THEN ct.user ELSE NULL END) AS Acquisitions,
       COUNT(DISTINCT CASE WHEN ct.cohort = 'M1' AND ct.orders > 0 THEN ct.user ELSE NULL END) AS M1_Users,
       SUM(CASE WHEN ct.cohort = 'M1' THEN ct.orders ELSE 0 END) AS M1_Orders,
       COUNT(DISTINCT CASE WHEN ct.cohort = 'M2' AND ct.orders > 0 THEN ct.user ELSE NULL END) AS M2_Users,
       SUM(CASE WHEN ct.cohort = 'M2' THEN ct.orders ELSE 0 END) AS M2_Orders,
       COUNT(DISTINCT CASE WHEN ct.cohort = 'M3' AND ct.orders > 0 THEN ct.user ELSE NULL END) AS M3_Users,
       SUM(CASE WHEN ct.cohort = 'M3' THEN ct.orders ELSE 0 END) AS M3_Orders,
       SUM(CASE WHEN ct.cohort = 'M1' THEN ct.total_paid ELSE 0 END) AS M1_Total_Paid,
       SUM(CASE WHEN ct.cohort = 'M1' THEN ct.total_disc ELSE 0 END) AS M1_Total_Disc,
       SUM(CASE WHEN ct.cohort = 'M1' THEN ct.total_voucher_disc ELSE 0 END) AS M1_Total_Voucher_Disc,
       SUM(CASE WHEN ct.cohort = 'M2' THEN ct.total_paid ELSE 0 END) AS M2_Total_Paid,
       SUM(CASE WHEN ct.cohort = 'M2' THEN ct.total_disc ELSE 0 END) AS M2_Total_Disc,
       SUM(CASE WHEN ct.cohort = 'M2' THEN ct.total_voucher_disc ELSE 0 END) AS M2_Total_Voucher_Disc,
       SUM(CASE WHEN ct.cohort = 'M3' THEN ct.total_paid ELSE 0 END) AS M3_Total_Paid,
       SUM(CASE WHEN ct.cohort = 'M3' THEN ct.total_disc ELSE 0 END) AS M3_Total_Disc,
       SUM(CASE WHEN ct.cohort = 'M3' THEN ct.total_voucher_disc ELSE 0 END) AS M3_Total_Voucher_Disc
FROM cohorts_table AS ct
GROUP BY 1,2,3,4'''

# Descargo la data
bq_tc = pd.io.gbq.read_gbq(q_tc, project_id='peya-argentina', dialect='standard')
bq_fl = pd.io.gbq.read_gbq(q_fl, project_id='peya-argentina', dialect='standard')
bq_st = pd.io.gbq.read_gbq(q_st, project_id='peya-argentina', dialect='standard')

# Copio las bases
tc = bq_tc.copy()
fl = bq_fl.copy()
st = bq_st.copy()

print('Finalizo Queries')

###########################################################
#
#       ACTIVE FLOWS
#
###########################################################

print('--------------------------------------------')
print('Arranca Active Flows')

### TRABAJO

# Doy formato a las columnas
fl = fl.apply(lambda x: x.astype(str).str.upper())
# Saco los espacios de los nombres de campañas
fl['Campaign'] = fl['Campaign'].str.replace(' ', '')

# Creo las columnas para los segmentos
fl['Benefit'] = fl['Campaign'].apply(lambda x: filtros(x,'BENEFICIOS'))
fl['Type'] = fl['Campaign'].apply(lambda x: filtros(x,'TIPOS'))
fl['Segment'] = fl['Campaign'].apply(lambda x: filtros(x,'SEGMENTOS'))
fl['Level'] = fl['Campaign'].apply(lambda x: filtros(x,'NIVELES'))
fl['Campaña'] = fl['Campaign'].apply(lambda x: filtros(x,'CAMPAIGNS'))
fl['Automated'] = fl['Campaign'].apply(lambda x: filtros(x,'AUTOMATIONS'))
fl['Budget'] = fl['Campaign'].apply(lambda x: filtros(x,'BUDGET'))
fl['Trial'] = fl['Campaign'].apply(lambda x: filtros(x,'TRIAL'))

# Marco las campañas a filtrar
fl['Filtrar'] = fl['Campaign'].apply(filtrar_cam)
fl = fl[fl['Filtrar'] == 'No'].copy()

# Divido los DF por Country
fl_arg = fl[fl['Country'] == 'ARGENTINA'].copy()
fl_chi = fl[fl['Country'] == 'CHILE'].copy()

### CARGA

# Carga Argentina
log = carga(fl_arg,'1_hOX88CV2BwFCPw37PH20116liF8nFdS4iEXLDL3LHM','Crudo Active','Crudo Active - Argentina',log)

time.sleep(30)

# Carga Chile
log = carga(fl_chi,'1CuWUY3WQhXHsZvpk7QMCEq9GJhSZU487as9_tFSBYF4','Crudo Active','Crudo Active - Chile',log)

print('Finalizo Active Flows')

###########################################################
#
#       FLOWS PERFORMANCE
#
###########################################################

print('--------------------------------------------')
print('Arranca Flows Performance')

### TRABAJO LOS CUPONES

# Doy formato a las columnas
cols_str = ['Campaign','Country','City']
cols_float = [i for i in tc.columns if i not in cols_str]
tc[cols_float] = tc[cols_float].astype(float)
tc[cols_str] = tc[cols_str].apply(lambda x: x.astype(str).str.upper())
# Saco los espacios de los nombres de campañas
tc['Campaign'] = tc['Campaign'].str.replace(' ', '')

# Creo las columnas para los segmentos
tc['Benefit'] = tc['Campaign'].apply(lambda x: filtros(x,'BENEFICIOS'))
tc['Type'] = tc['Campaign'].apply(lambda x: filtros(x,'TIPOS'))
tc['Segment'] = tc['Campaign'].apply(lambda x: filtros(x,'SEGMENTOS'))
tc['Level'] = tc['Campaign'].apply(lambda x: filtros(x,'NIVELES'))
tc['Campaña'] = tc['Campaign'].apply(lambda x: filtros(x,'CAMPAIGNS'))
tc['Automated'] = tc['Campaign'].apply(lambda x: filtros(x,'AUTOMATIONS'))
tc['Budget'] = tc['Campaign'].apply(lambda x: filtros(x,'BUDGET'))
tc['Trial'] = tc['Campaign'].apply(lambda x: filtros(x,'TRIAL'))
# Coloco el IVA
tc = func_iva(tc)

# Marco las campañas a filtrar
tc['Filtrar'] = tc['Campaign'].apply(filtrar_cam)
tc = tc[tc['Filtrar'] == 'No'].copy()

# Saco el IVA
used_usd = 'Used_Amount_Confirmed_USD'
used_shopper_usd = 'Used_Amount_Confirmed_USD_Shopper'
used = 'Used_Amount_Confirmed'
used_shopper = 'Used_Amount_Confirmed_Shopper'
tc['Used_Final_USD'] = tc[used_shopper_usd] / (1 + tc['IVA']) * tc['Extra'] + tc[used_usd]
tc['Used_Final'] = tc[used_shopper] / (1 + tc['IVA']) * tc['Extra'] + tc[used]

### TRABAJO STATS

# Doy formato a las columnas
cols_str = ['Campaign','Country','Month','City']
cols_float = [i for i in st.columns if i not in cols_str]
st[cols_float] = st[cols_float].astype(float)
st[cols_str] = st[cols_str].apply(lambda x: x.astype(str).str.upper())
# Saco los espacios de los nombres de campañas
st['Campaign'] = st['Campaign'].str.replace(' ', '')
# Doy formato a la fecha
st['Month'] = pd.to_datetime(st['Month'], format='%Y-%m').dt.strftime('%Y-%m')
# Ordeno la base
st.sort_values(by=['Country','Month','Campaign','City'],inplace=True)

# Llevo a cabo la limpieza del Reorder
cols_final = ['M1_Users_Final','M2_Users_Final','M3_Users_Final',
              'M1_Orders_Final','M2_Orders_Final','M3_Orders_Final',
              'Acq_M1','Acq_M2','Acq_M3']

for i in cols_final:
    st[i] = 0
# Completo las columnas que cree
cols_toFill_m1 = ['M1_Users_Final','M1_Orders_Final','Acq_M1']
cols_fill_m1 = ['M1_Users','M1_Orders','Acquisitions']
cols_toFill_m2 = ['M2_Users_Final','M2_Orders_Final','Acq_M2']
cols_fill_m2 = ['M2_Users','M2_Orders','Acquisitions']
cols_toFill_m3 = ['M3_Users_Final','M3_Orders_Final','Acq_M3']
cols_fill_m3 = ['M3_Users','M3_Orders','Acquisitions']
months = st.sort_values(by=['Month'],ascending=False)['Month'].unique().tolist()
if len(months) > 3:
    # Para todos los meses coloco los valores en M1
    st[cols_toFill_m1] = st[cols_fill_m1].values
    # Para el tercer y cuarto mes coloco los valores en M2
    st.loc[st['Month'].isin(months[2:]),cols_toFill_m2] = st[st['Month'].isin(months[2:])][cols_fill_m2].values
    # Para el cuarto mes coloco los valores en M3
    st.loc[st['Month'] == months[-1],cols_toFill_m3] = st[st['Month'] == months[-1]][cols_fill_m3].values
else:
    # Para todos los meses coloco los valores en M1
    st[cols_toFill_m1] = st[cols_fill_m1].values
    # Para el segundo mes coloco los valores en M2
    st.loc[st['Month'].isin(months[1:]),cols_toFill_m2] = st[st['Month'].isin(months[1:])][cols_fill_m2].values
    # Para el cuarto mes coloco los valores en M3
    st.loc[st['Month'] == months[-1],cols_toFill_m3] = st[st['Month'] == months[-1]][cols_fill_m3].values

# Hago una PT sacando el Month
index = ['Country','City','Campaign']
values = [i for i in st.columns if i not in index+['Month']]
pt_st = st.pivot_table(index=index,values=values,aggfunc='sum').reset_index()

# Creo las columnas para los segmentos
pt_st['Benefit'] = pt_st['Campaign'].apply(lambda x: filtros(x,'BENEFICIOS'))
pt_st['Type'] = pt_st['Campaign'].apply(lambda x: filtros(x,'TIPOS'))
pt_st['Segment'] = pt_st['Campaign'].apply(lambda x: filtros(x,'SEGMENTOS'))
pt_st['Level'] = pt_st['Campaign'].apply(lambda x: filtros(x,'NIVELES'))
pt_st['Campaña'] = pt_st['Campaign'].apply(lambda x: filtros(x,'CAMPAIGNS'))
pt_st['Automated'] = pt_st['Campaign'].apply(lambda x: filtros(x,'AUTOMATIONS'))
pt_st['Budget'] = pt_st['Campaign'].apply(lambda x: filtros(x,'BUDGET'))
pt_st['Trial'] = pt_st['Campaign'].apply(lambda x: filtros(x,'TRIAL'))

# Marco las campañas a filtrar
pt_st['Filtrar'] = pt_st['Campaign'].apply(filtrar_cam)
pt_st = pt_st[pt_st['Filtrar'] == 'No'].copy()

### UNION

# Merge entre TC y Stats
on = ['Country','City','Campaign']
cols = ['Country','City','Campaign','Acq_M1','Acq_M2','Acq_M3','Acquisitions','M1_Orders','M1_Orders_Final','M1_Total_Disc',
        'M1_Total_Paid','M1_Total_Voucher_Disc','M1_Users','M1_Users_Final','M2_Orders','M2_Orders_Final','M2_Total_Disc','M2_Total_Paid',
        'M2_Total_Voucher_Disc','M2_Users','M2_Users_Final','M3_Orders','M3_Orders_Final','M3_Total_Disc','M3_Total_Paid',
        'M3_Total_Voucher_Disc','M3_Users','M3_Users_Final','Original_Amount_Used']
final = tc.merge(pt_st[cols],on=on,how='left').copy()
final.replace([np.nan,np.inf,-np.inf],'-',inplace=True)

# Ordeno las columnas
cols = ['Campaign','Country','City','Type','Segment','Level','Campaña','Automated','Budget','Trial']
cols_resto = [i for i in final.columns if i not in cols]
final = final[cols+cols_resto].copy()

# Divido por pais
cols_sort = ['City','Campaign']
final_arg = final[final['Country'] == 'ARGENTINA'].sort_values(by=cols_sort,ascending=False).copy()
final_chi = final[final['Country'] == 'CHILE'].sort_values(by=cols_sort,ascending=False).copy()
# Dropeo la columna country para achicar el DF
final_arg.drop(['Country'],axis=1,inplace=True)
final_chi.drop(['Country'],axis=1,inplace=True)

### TIPOS DE CAMPAÑAS

# Creo un DF con las unique combinations
cols = ['Budget','Type','Segment','Trial','Level','Campaña','Automated','Country']
campaign_type = final.groupby(cols).size().reset_index().drop([0], axis = 1)
# Ordeno el DF
cols = ['Budget','Type','Segment']
campaign_type.sort_values(by=cols,ascending=False,inplace=True)
# Divido por pais
ct_arg = campaign_type[campaign_type['Country'] == 'ARGENTINA'].copy()
ct_chi = campaign_type[campaign_type['Country'] == 'CHILE'].copy()

### CARGA

# Carga Argentina
log = carga(final_arg,'1_hOX88CV2BwFCPw37PH20116liF8nFdS4iEXLDL3LHM','Crudo Performance','Crudo Performance - Argentina',log)

time.sleep(60)

log = carga(ct_arg,'1_hOX88CV2BwFCPw37PH20116liF8nFdS4iEXLDL3LHM','Filtro Campaigns','Filtro Campaigns - Argentina',log)

# Carga Chile
log = carga(final_chi,'1CuWUY3WQhXHsZvpk7QMCEq9GJhSZU487as9_tFSBYF4','Crudo Performance','Crudo Active - Chile',log)

time.sleep(60)

log = carga(ct_chi,'1CuWUY3WQhXHsZvpk7QMCEq9GJhSZU487as9_tFSBYF4','Filtro Campaigns','Filtro Campaigns - Chile',log)

print('Finalizo Flows Performance')

print('Finalizo DASHBOARD PAISES')

###############################################################################
#                                                                             #
#                              TABLERO GENERAL                                #
#                                                                             #
###############################################################################

print('--------------------------------------------')
print('Arranca TABLERO GENERAL')

###########################################################
#
#       QUERIES
#
###########################################################

print('--------------------------------------------')
print('Arranca Queries')

q_crudo = '''WITH coupons_table AS (
    SELECT cn.country_name AS country,
           tc.talon_campaign_id AS campaign_id,
           tc.coupon_id AS coupon_id,
           o.registered_date AS fecha,
           CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN 'Si' ELSE 'No' END AS payment_shopper,
           COUNT(DISTINCT o.order_id) AS redeemed,
           COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.order_id ELSE NULL END) AS redeemed_confirmed,
           COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS acquisitions,
           SUM(tc.coupon_used_amount) AS used_amount,
           SUM(tc.coupon_used_amount / ce.rate_us) AS used_amount_usd,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN tc.coupon_used_amount ELSE 0 END) AS used_amount_confirmed,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS used_amount_confirmed_usd
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
    INNER JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` AS tc ON o.order_id = tc.order_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON o.restaurant.id = p.partner_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_billing_info` AS bi ON p.billingInfo.billing_info_id = bi.billing_info_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON tc.coupon_country_id = cn.country_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce ON cn.currency_id = ce.currency_id AND DATE_TRUNC(o.registered_date,MONTH) = ce.currency_exchange_date
    WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
          AND DATE(tc.coupon_created_at) >= DATE('{2}')
    GROUP BY 1,2,3,4,5)
SELECT ct.fecha AS Fecha,
       ct.country AS Country,
       tca.talon_campaign_name AS Campaign,
       ct.payment_shopper AS Payment_Shopper,
       SUM(ct.redeemed) AS Redeemed,
       SUM(ct.redeemed_confirmed) AS Redeemed_Confirmed,
       SUM(ct.acquisitions) AS Acq,
       SUM(ct.used_amount) AS Used_Amount,
       SUM(ct.used_amount_usd) AS Used_Amount_USD,
       SUM(ct.used_amount_confirmed) AS Used_Amount_Confirmed,
       SUM(ct.used_amount_confirmed_usd) AS Used_Amount_Confirmed_USD
FROM coupons_table AS ct
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` AS tca ON ct.campaign_id = tca.talon_campaign_id
GROUP BY 1,2,3,4'''.format(start,end,start_tc)

q_wallet = '''WITH wallet_table AS (
    SELECT wa.order_id AS order_id,
           d.campaignCode AS campaign,
           d.amount AS amount
    FROM `peya-bi-tools-pro.il_wallet.fact_wallet_attributions` AS wa,
    UNNEST (attributionDetails) AS d
    WHERE wa.operation = 'Purchase'),
    coupons_table AS (
    SELECT cn.country_name AS country,
           o.registered_date AS fecha,
           wa.campaign AS campaign,
           CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN 'Si' ELSE 'No' END AS payment_shopper,
           COUNT(DISTINCT o.order_id) AS redeemed,
           COUNT(DISTINCT CASE WHEN o.order_status = 'CONFIRMED' THEN o.order_id ELSE NULL END) AS redeemed_confirmed,
           COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS acquisitions,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.registered_date < DATE('2020-06-09') THEN w.amount
                    WHEN o.order_status = 'CONFIRMED' AND o.registered_date >= DATE('2020-06-09') THEN wa.amount
                    ELSE 0 END) AS used_amount_confirmed,
           SUM(CASE WHEN o.order_status = 'CONFIRMED' AND o.registered_date < DATE('2020-06-09') THEN w.amount / ce.rate_us
                    WHEN o.order_status = 'CONFIRMED' AND o.registered_date >= DATE('2020-06-09') THEN wa.amount / ce.rate_us
                    ELSE 0 END) AS used_amount_confirmed_usd,
           SUM(CASE WHEN o.registered_date < DATE('2020-06-09') THEN w.amount
                    WHEN o.registered_date >= DATE('2020-06-09') THEN wa.amount
                    ELSE 0 END) AS used_amount,
           SUM(CASE WHEN o.registered_date < DATE('2020-06-09') THEN w.amount / ce.rate_us
                    WHEN o.registered_date >= DATE('2020-06-09') THEN wa.amount / ce.rate_us
                    ELSE 0 END) AS used_amount_usd
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
    INNER JOIN `peya-bi-tools-pro.il_core.fact_order_funding` AS w ON o.order_id = w.order_id AND w.type IN (25,35)
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON o.restaurant.id = p.partner_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_billing_info` AS bi ON p.billingInfo.billing_info_id = bi.billing_info_id
    LEFT JOIN wallet_table AS wa ON o.order_id = wa.order_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce ON cn.currency_id = ce.currency_id AND DATE_TRUNC(o.registered_date,MONTH) = ce.currency_exchange_date
    WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
    GROUP BY 1,2,3,4)
SELECT ct.fecha AS Fecha,
       ct.country AS Country,
       ct.campaign AS Campaign,
       ct.payment_shopper AS Payment_Shopper,
       SUM(ct.redeemed) AS Redeemed,
       SUM(ct.redeemed_confirmed) AS Redeemed_Confirmed,
       SUM(ct.acquisitions) AS Acq,
       SUM(ct.used_amount) AS Used_Amount,
       SUM(ct.used_amount_usd) AS Used_Amount_USD,
       SUM(ct.used_amount_confirmed) AS Used_Amount_Confirmed,
       SUM(ct.used_amount_confirmed_usd) AS Used_Amount_Confirmed_USD
FROM coupons_table AS ct
GROUP BY 1,2,3,4'''.format(start,end)

q_ord = '''SELECT cn.country_name AS Country,
       o.registered_date AS Fecha,
       COUNT(DISTINCT o.order_id) AS Orders,
       COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS Acq,
       COUNT(DISTINCT o.user.id) AS Active_Users,
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.order_status = 'CONFIRMED'
GROUP BY 1,2'''.format(start,end)

# Descargo la data
bq_crudo = pd.io.gbq.read_gbq(q_crudo, project_id='peya-argentina', dialect='standard')
bq_wallet = pd.io.gbq.read_gbq(q_wallet, project_id='peya-argentina', dialect='standard')
bq_ord = pd.io.gbq.read_gbq(q_ord, project_id='peya-argentina', dialect='standard')

# Copio las bases
crudo = bq_crudo.copy()
wallet = bq_wallet.copy()
orders = bq_ord.copy()

print('Finalizo Queries')

###########################################################
#
#       TRABAJO
#
###########################################################

print('--------------------------------------------')
print('Arranca Trabajo')

### TRABAJO ORDERS

# Doy formato a las columnas
cols_str = ['Fecha','Country']
cols_float = [i for i in orders.columns if i not in cols_str]
orders[cols_float] = orders[cols_float].astype(float)
orders[cols_str] = orders[cols_str].apply(lambda x: x.astype(str).str.upper())
# Doy formato a la fecha
orders['Fecha'] = pd.to_datetime(orders['Fecha'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
orders['Month'] = pd.to_datetime(orders['Fecha'], format='%Y-%m').dt.strftime('%Y-%m')
orders['Week'] = pd.to_datetime(orders['Fecha'], format='%Y-%m-%d').dt.strftime('%V')

# Marco las de la ultima semana completa
orders.loc[(orders['Fecha'] >= inicio)&(orders['Fecha'] <= fin),'Week Type'] = 'Last Complete Week'
orders.loc[orders['Fecha'] > fin,'Week Type'] = 'Current Week'
orders.loc[orders['Week Type'].isna(),'Week Type'] = 'Week Before'

# Creo una PT sin fecha
index = ['Country','Month','Week','Week Type']
values = ['Orders','Acq','Active_Users']
pt_orders = orders.pivot_table(index=index,values=values,aggfunc='sum',fill_value=0).reset_index()

# Ordeno la PT Final
pt_orders.sort_values(by=['Month','Week','Country'],inplace=True,ascending=True)
# Marco las semanas finalizadas
pt_orders['Week Finished'] = pt_orders['Week Type'].apply(lambda x: 'Si' if x != 'Current Week' else 'No')

### TRABAJO CRUDO

# Doy formato a las columnas
cols_str = ['Fecha','Country','Campaign','Payment_Shopper']
cols_float = [i for i in crudo.columns if i not in cols_str]
crudo[cols_float] = crudo[cols_float].astype(float)
crudo[cols_str] = crudo[cols_str].apply(lambda x: x.astype(str).str.upper())
# Saco los espacios de los nombres de campañas
crudo['Campaign'] = crudo['Campaign'].str.replace(' ', '')
# Doy formato a la fecha
crudo['Fecha'] = pd.to_datetime(crudo['Fecha'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
crudo['Month'] = pd.to_datetime(crudo['Fecha'], format='%Y-%m').dt.strftime('%Y-%m')
crudo['Week'] = pd.to_datetime(crudo['Fecha'], format='%Y-%m-%d').dt.strftime('%V')

# Marco las de la ultima semana completa
crudo.loc[(crudo['Fecha'] >= inicio)&(crudo['Fecha'] <= fin),'Week Type'] = 'Last Complete Week'
crudo.loc[crudo['Fecha'] > fin,'Week Type'] = 'Current Week'
crudo.loc[crudo['Week Type'].isna(),'Week Type'] = 'Week Before'

# Creo las columnas para los segmentos
crudo['Budget'] = crudo['Campaign'].apply(lambda x: filtros(x,'BUDGET'))
crudo['Type'] = crudo['Campaign'].apply(lambda x: filtros(x,'TIPOS'))
# Marco las campañas a filtrar
crudo['Filtrar'] = crudo['Campaign'].apply(filtrar_cam)
crudo = crudo[crudo['Filtrar'] == 'No'].copy()

### TRABAJO WALLET

# Doy formato a las columnas
cols_str = ['Fecha','Country','Campaign','Payment_Shopper']
cols_float = [i for i in wallet.columns if i not in cols_str]
wallet[cols_float] = wallet[cols_float].astype(float)
wallet[cols_str] = wallet[cols_str].apply(lambda x: x.astype(str).str.upper())
# Saco los espacios de los nombres de campañas
wallet['Campaign'] = wallet['Campaign'].str.replace(' ', '')
# Doy formato a la fecha
wallet['Fecha'] = pd.to_datetime(wallet['Fecha'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
wallet['Month'] = pd.to_datetime(wallet['Fecha'], format='%Y-%m').dt.strftime('%Y-%m')
wallet['Week'] = pd.to_datetime(wallet['Fecha'], format='%Y-%m-%d').dt.strftime('%V')

# Marco las de la ultima semana completa
wallet.loc[(wallet['Fecha'] >= inicio)&(wallet['Fecha'] <= fin),'Week Type'] = 'Last Complete Week'
wallet.loc[wallet['Fecha'] > fin,'Week Type'] = 'Current Week'
wallet.loc[wallet['Week Type'].isna(),'Week Type'] = 'Week Before'

# Creo las columnas para los segmentos
wallet['Budget'] = wallet['Campaign'].apply(lambda x: filtros(x,'BUDGET'))
wallet['Type'] = 'WALLET'
# Marco las campañas a filtrar
wallet['Filtrar'] = wallet['Campaign'].apply(filtrar_cam)
wallet = wallet[wallet['Filtrar'] == 'No'].copy()

### FINAL

# Uno ambos DF
final = pd.concat([crudo,wallet]).copy()
# Creo una PT
index = ['Month','Week','Week Type','Country','Payment_Shopper','Type','Budget']
values = ['Redeemed','Redeemed_Confirmed','Acq','Used_Amount','Used_Amount_USD',
          'Used_Amount_Confirmed','Used_Amount_Confirmed_USD']
pt = final.pivot_table(index=index,values=values,aggfunc='sum',fill_value=0).reset_index()

# Coloco el IVA
pt = func_iva(pt)
# Saco el IVA
used = 'Used_Amount_Confirmed'
used_usd = 'Used_Amount_Confirmed_USD'
pt.loc[pt['Payment_Shopper'] == 'NO','Used_Final'] = pt[used] / (1 + pt['IVA']) * pt['Extra']
pt.loc[pt['Payment_Shopper'] == 'SI','Used_Final'] = pt[used]
pt.loc[pt['Payment_Shopper'] == 'NO','Used_USD_Final'] = pt[used_usd] / (1 + pt['IVA']) * pt['Extra']
pt.loc[pt['Payment_Shopper'] == 'SI','Used_USD_Final'] = pt[used_usd]

# Ordeno la PT Final
pt.sort_values(by=['Month','Week','Country'],inplace=True,ascending=True)
# Marco las semanas finalizadas
pt['Week Finished'] = pt['Week Type'].apply(lambda x: 'Si' if x != 'Current Week' else 'No')

### CARGA

# Carga Crudo Total
log = carga(pt_orders,'1y3HUUDmt73Z3z6rE_KefkFjySSVNfChQwCm0-aUpoq0','Crudo General','Tablero General - Crudo General',log)

# Carga Crudo CRM
log = carga(pt,'1y3HUUDmt73Z3z6rE_KefkFjySSVNfChQwCm0-aUpoq0','Crudo','Tablero General - Crudo CRM',log)

print('Finalizo Trabajo')

print('Finalizo TABLERO GENERAL')

###############################################################################
#                                                                             #
#                               OBJETIVOS CRM                                 #
#                                                                             #
###############################################################################

print('--------------------------------------------')
print('Arranca OBJETIVOS CRM')

###########################################################
#
#       QUERIES
#
###########################################################

print('--------------------------------------------')
print('Arranca Queries')

q_ord = '''SELECT cn.country_name AS Country,
       COUNT(DISTINCT o.order_id) AS Orders,
       COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS Acq,
       COUNT(DISTINCT o.user.id) AS Active_Users,
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.order_status = 'CONFIRMED'
GROUP BY 1'''.format(itm,ftm)

q_m1 = '''WITH users_table AS (
    SELECT cn.country_name AS country,
           o.user.id AS user,
           SUM(o.is_acquisition) AS acq
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    WHERE o.registered_date BETWEEN DATE('{2}') AND DATE('{3}')
          AND o.order_status = 'CONFIRMED'
    GROUP BY 1,2),
    reorder_table AS (
    SELECT ut.country AS country,
           ut.user AS user,
           ut.acq AS acq
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o 
    INNER JOIN users_table AS ut ON o.user.id = ut.user
    WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
          AND o.order_status = 'CONFIRMED'
    GROUP BY 1,2,3)
SELECT ut.country AS Country,
       COUNT(DISTINCT CASE WHEN ut.acq > 0 THEN ut.user ELSE NULL END) AS Acq_LM,
       COUNT(DISTINCT CASE WHEN ut.acq = 0 THEN ut.user ELSE NULL END) AS Active_LM,
       COUNT(DISTINCT CASE WHEN ut.acq > 0 THEN rt.user ELSE NULL END) AS Acq_Reorder,
       COUNT(DISTINCT CASE WHEN ut.acq = 0 THEN rt.user ELSE NULL END) AS Active_Reorder
FROM users_table AS ut
LEFT JOIN reorder_table AS rt ON ut.country = rt.country AND ut.user = rt.user
GROUP BY 1'''.format(itm,ftm,ilm,flm)

q_trials = '''WITH orders_table AS (
    SELECT o.user.id AS user,
           o.registered_date AS fecha,
           bt.business_type_name AS business,
           CASE WHEN o.is_acquisition = 1 THEN TRUE ELSE FALSE END AS acq,
           cn.country_name AS country
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o 
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_business_type` AS bt ON o.business_type_id = bt.business_type_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    WHERE o.registered_date < CURRENT_DATE()
          AND o.order_status = 'CONFIRMED'),
    qc_table AS (
    SELECT ot.user AS user,
           ot.business AS business,
           ot.country AS country,
           ot.acq AS acq,
    FROM orders_table AS ot
    WHERE ot.business NOT IN ('Restaurant','Coffee','Courier','Courier Business')
          AND ot.fecha BETWEEN DATE('{0}') AND DATE('{1}')),
    history_table AS (
    SELECT ot.user AS user,
           ot.country AS country,
           SUM(CASE WHEN ot.business NOT IN ('Restaurant','Coffee','Courier','Courier Business') THEN 1 ELSE 0 END) AS qc
    FROM orders_table AS ot
    WHERE ot.fecha < DATE('{0}')
          OR ot.acq
    GROUP BY 1,2)
SELECT qct.country AS Country,
       COUNT(DISTINCT qct.user) AS Active_CQ,
       COUNT(DISTINCT CASE WHEN qct.acq = FALSE AND (ht.qc = 0 OR ht.qc IS NULL) THEN qct.user ELSE NULL END) AS Adq_QC
FROM qc_table AS qct
LEFT JOIN history_table AS ht ON qct.user = ht.user AND qct.country = ht.country
GROUP BY 1
ORDER BY 1'''.format(itm,ftm)

q_desired = '''WITH orders_table AS (
    SELECT o.registered_date AS fecha,
           cn.country_name AS country,
           o.order_id AS order_id,
           o.user.id AS user,
           IFNULL(o.shipping_amount_no_discount + o.amount_no_discount,0) AS total_paid,
           SUM(IFNULL(od.discount_amount,0)) AS total_disc
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o,
    UNNEST(discounts) AS od
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    WHERE o.registered_date BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(),INTERVAL -6 MONTH),MONTH) AND LAST_DAY(DATE_ADD(CURRENT_DATE(),INTERVAL -1 MONTH))
          AND o.order_status = 'CONFIRMED'
    GROUP BY 1,2,3,4,5),
    users_table AS (
    SELECT ot.user AS user,
           ot.country AS country,
           COUNT(DISTINCT ot.order_id) AS orders,
           SAFE_DIVIDE(SUM(ot.total_disc),SUM(total_paid)) AS adr,
           MAX(ot.fecha) AS last_order,
           CASE WHEN MAX(ot.fecha) <= LAST_DAY(DATE_ADD(CURRENT_DATE(),INTERVAL -2 MONTH)) AND SAFE_DIVIDE(SUM(ot.total_disc),SUM(total_paid)) < 0.3 THEN 'Si' ELSE 'No' END AS desired
    FROM orders_table AS ot
    GROUP BY 1,2),
    returns_table AS (
    SELECT cn.country_name AS country,
           ut.user AS user,
           COUNT(DISTINCT o.order_id) AS orders
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    INNER JOIN users_table AS ut ON o.user.id = ut.user
    WHERE o.registered_date BETWEEN DATE_TRUNC(CURRENT_DATE(),MONTH) AND LAST_DAY(CURRENT_DATE())
          AND o.order_status = 'CONFIRMED'
          AND ut.desired = 'Si'
    GROUP BY 1,2)
SELECT ut.country AS Country,
       COUNT(DISTINCT ut.user) AS Desired,
       COUNT(DISTINCT rt.user) AS Return_Desired,
       SUM(rt.orders) AS Return_Desired_Orders
FROM users_table AS ut
LEFT JOIN returns_table AS rt ON ut.user = rt.user AND ut.country = rt.country
WHERE ut.desired = 'Si'
GROUP BY 1'''

q_crm = '''WITH wallet_table AS (
    SELECT DISTINCT wa.order_id AS order_id,
           d.campaignCode AS campaign,
           d.amount AS amount
    FROM `peya-bi-tools-pro.il_wallet.fact_wallet_attributions` AS wa,
    UNNEST (attributionDetails) AS d
    WHERE wa.operation = 'Purchase'),
    wc_table AS (
    SELECT cn.country_name AS country,
           o.registered_date AS fecha,
           wa.campaign AS campaign,
           COUNT(DISTINCT o.order_id) AS orders,
           COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS acq,
           SUM(CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN wa.amount / ce.rate_us ELSE 0 END) AS amount,
           SUM(CASE WHEN bi.payment_mode != 'TOTAL_AMOUNT' THEN wa.amount / ce.rate_us ELSE 0 END) AS amount_shopper
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
    INNER JOIN `peya-bi-tools-pro.il_core.fact_order_funding` AS w ON o.order_id = w.order_id AND w.type IN (25,35)
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON o.restaurant.id = p.partner_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_billing_info` AS bi ON p.billingInfo.billing_info_id = bi.billing_info_id
    LEFT JOIN wallet_table AS wa ON o.order_id = wa.order_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce ON cn.currency_id = ce.currency_id AND DATE_TRUNC(o.registered_date,MONTH) = ce.currency_exchange_date
    WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
          AND o.order_status = 'CONFIRMED'
    GROUP BY 1,2,3)
SELECT wct.country AS Country,
       wct.campaign AS Campaign,
       'WALLET' AS Benefit,
       SUM(wct.orders) AS Orders_CRM,
       SUM(wct.acq) AS Acq_CRM,
       SUM(wct.amount) AS Amount,
       SUM(wct.amount_shopper) AS Amount_Shopper
FROM wc_table AS wct
GROUP BY 1,2

UNION ALL

SELECT cn.country_name AS Country,
       tca.talon_campaign_name AS Campaign,
       'TALON COUPON' AS Benefit,
       COUNT(DISTINCT o.order_id) AS Orders_CRM,
       COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS Acq_CRM,
       SUM(CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS Amount,
       SUM(CASE WHEN bi.payment_mode != 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS Amount_Shopper
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
INNER JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` AS tc ON o.order_id = tc.order_id
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` AS tca ON tc.talon_campaign_id = tca.talon_campaign_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce ON cn.currency_id = ce.currency_id AND DATE_TRUNC(o.registered_date,MONTH) = ce.currency_exchange_date
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON o.restaurant.id = p.partner_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_billing_info` AS bi ON p.billingInfo.billing_info_id = bi.billing_info_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.order_status = 'CONFIRMED'
GROUP BY 1,2'''.format(itm,ftm)

q_react = '''WITH orders_table AS (
    SELECT o.registered_date AS fecha,
           cn.country_name AS country,
           o.order_id AS order_id,
           o.user.id AS user
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o,
    UNNEST(discounts) AS od
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    WHERE o.registered_date <= CURRENT_DATE()
          AND o.order_status = 'CONFIRMED'),
    users_table AS (
    SELECT ot.user AS user,
           ot.country AS country,
           COUNT(DISTINCT ot.order_id) AS orders,
           MAX(ot.fecha) AS last_order,
           CASE WHEN MAX(ot.fecha) <= LAST_DAY(DATE_ADD(CURRENT_DATE(),INTERVAL -2 MONTH)) THEN 'Si' ELSE 'No' END AS reactivate
    FROM orders_table AS ot
    WHERE ot.fecha <= LAST_DAY(DATE_ADD(CURRENT_DATE(),INTERVAL -1 MONTH))
    GROUP BY 1,2),
    returns_table AS (
    SELECT ot.country AS country,
           ut.user AS user,
           COUNT(DISTINCT ot.order_id) AS orders
    FROM orders_table AS ot
    INNER JOIN users_table AS ut ON ot.user = ut.user
    WHERE ot.fecha BETWEEN DATE_TRUNC(CURRENT_DATE(),MONTH) AND LAST_DAY(CURRENT_DATE())
          AND ut.reactivate = 'Si'
    GROUP BY 1,2)
SELECT ut.country AS Country,
       COUNT(DISTINCT ut.user) AS Reactivated,
       COUNT(DISTINCT rt.user) AS Return_Reactivated,
       SUM(rt.orders) AS Return_Reactivated_Orders
FROM users_table AS ut
LEFT JOIN returns_table AS rt ON ut.user = rt.user AND ut.country = rt.country
WHERE ut.reactivate = 'Si'
GROUP BY 1'''

# Descargo la data
bq_ord = pd.io.gbq.read_gbq(q_ord, project_id='peya-argentina', dialect='standard')
bq_m1 = pd.io.gbq.read_gbq(q_m1, project_id='peya-argentina', dialect='standard')
bq_trials = pd.io.gbq.read_gbq(q_trials, project_id='peya-argentina', dialect='standard')
bq_desired = pd.io.gbq.read_gbq(q_desired, project_id='peya-argentina', dialect='standard')
bq_crm = pd.io.gbq.read_gbq(q_crm, project_id='peya-argentina', dialect='standard')
bq_react = pd.io.gbq.read_gbq(q_react, project_id='peya-argentina', dialect='standard')

# Copio la data
orders = bq_ord.copy()
m1 = bq_m1.copy()
trials = bq_trials.copy()
desired = bq_desired.copy()
crm = bq_crm.copy()
react = bq_react.copy()

print('Finalizo Queries')

###########################################################
#
#       TRABAJO
#
###########################################################

print('--------------------------------------------')
print('Arranca Trabajo')

### TRABAJO

# Trabajo Orders
val = [i for i in orders.columns if i not in ['Country']]
orders[val] = orders[val].astype(float)
col = ['Country']
orders[col] = orders[col].apply(lambda x: x.astype(str).str.upper())
# Trabajo M1
val = [i for i in m1.columns if i not in ['Country']]
m1[val] = m1[val].astype(float)
col = ['Country']
m1[col] = m1[col].apply(lambda x: x.astype(str).str.upper())
# Trabajo Trials
val = [i for i in trials.columns if i not in ['Country']]
trials[val] = trials[val].astype(float)
col = ['Country']
trials[col] = trials[col].apply(lambda x: x.astype(str).str.upper())
# Trabajo Desired
val = [i for i in desired.columns if i not in ['Country']]
desired[val] = desired[val].astype(float)
col = ['Country']
desired[col] = desired[col].apply(lambda x: x.astype(str).str.upper())
# Trabajo CRM
val = [i for i in crm.columns if i not in ['Country','Campaign','Benefit']]
crm[val] = crm[val].astype(float)
col = ['Country','Campaign','Benefit']
crm[col] = crm[col].apply(lambda x: x.astype(str).str.upper())
crm['Campaign'] = crm['Campaign'].str.replace(' ', '')
# Trabajo React
val = [i for i in react.columns if i not in ['Country']]
react[val] = react[val].astype(float)
col = ['Country']
react[col] = react[col].apply(lambda x: x.astype(str).str.upper())

# Trabajo CRM
crm['Type'] = crm.apply(lambda x: filtros(x['Campaign'],'TIPOS',x['Benefit']),axis=1)
crm['Segment'] = crm['Campaign'].apply(lambda x: filtros(x,'SEGMENTOS'))
crm['Level'] = crm['Campaign'].apply(lambda x: filtros(x,'NIVELES'))
crm['Campaña'] = crm['Campaign'].apply(lambda x: filtros(x,'CAMPAIGNS'))
crm['Automated'] = crm['Campaign'].apply(lambda x: filtros(x,'AUTOMATIONS'))
crm['Budget'] = crm['Campaign'].apply(lambda x: filtros(x,'BUDGET'))
crm['Trial'] = crm['Campaign'].apply(lambda x: filtros(x,'TRIAL'))
# Coloco el IVA
crm = func_iva(crm)
# Marco las campañas a filtrar
crm['Filtrar'] = crm['Campaign'].apply(filtrar_cam)
crm = crm[crm['Filtrar'] == 'No'].copy()
# Saco el IVA
used = 'Amount'
used_shopper = 'Amount_Shopper'
crm['Final'] = crm[used_shopper] / (1 + crm['IVA']) * crm['Extra'] + crm[used]

# Hago una PT de CRM
index = ['Country','Type','Budget','Trial']
values = ['Orders_CRM','Acq_CRM','Final']
final_crm = crm.pivot_table(index=index,values=values,aggfunc='sum',fill_value=0).reset_index()

# Hago el merge para unir las tablas
final = orders.merge(m1,on=['Country'],how='outer').copy()
final = final.merge(trials,on=['Country'],how='outer').copy()
final = final.merge(desired,on=['Country'],how='outer').copy()
final = final.merge(react,on=['Country'],how='outer').copy()
final.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Creo las columnas necesarias
final['M1_Adq'] = final['Acq_Reorder'] / final['Acq_LM']
final['No_Return'] = (final['Active_LM'] - final['Active_Reorder']) / final['Active_LM']
final['%Trials'] = final['Adq_QC'] / final['Active_CQ']
final['%Return Desired'] = final['Return_Desired'] / final['Desired']
# Creo los RR
final['Orders RR'] = round(final['Orders'] / rr,0)
final['Acq RR'] = round(final['Acq'] / rr,0)
final['Adq_QC RR'] = round(final['Adq_QC'] / rr,0)
final['Reactivated RR'] = round(final['Return_Reactivated'] / rr,0)
final['Return_Desired RR'] = round(final['Return_Desired'] / rr,0)
final['RR'] = rr
final.replace([np.nan,np.inf,-np.inf],0,inplace=True)
final_crm['Orders_CRM RR'] = round(final_crm['Orders_CRM'] / rr)
final_crm['Acq_CRM RR'] = round(final_crm['Acq_CRM'] / rr)
final_crm['Final RR'] = round(final_crm['Final'] / rr)
final_crm['RR'] = rr
final_crm.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Hago un sort por Country
final.sort_values(['Country'],ascending=True,inplace=True)
final_crm.sort_values(['Country'],ascending=True,inplace=True)

# Carga Final
log = carga(final,'1SRFsNmj6hx_P0p98u1jM7AQJp0EfaOLdiA-jpHnduLg','Crudo BC','Crudo Objetivos',log)

# Carga Final CRM
log = carga(final_crm,'1SRFsNmj6hx_P0p98u1jM7AQJp0EfaOLdiA-jpHnduLg','Crudo CRM BC','Crudo Objetivos CRM',log)

print('Finaliza Trabajo')

print('Finalizo OBJETIVOS CRM')

###############################################################################
#                                                                             #
#                             GASTO POR ZONAS                                 #
#                                                                             #
###############################################################################

print('--------------------------------------------')
print('Arranca GASTO POR ZONAS')

###########################################################
#
#       CONSTANTES
#
###########################################################

# Fechas
today = datetime.date.today()
if today.day == 1:
    fin = str(today - relativedelta(days=1))
    inicio = str((today - relativedelta(days=1) - relativedelta(months=2)).replace(day=1))
else:
    fin = str(today + relativedelta(months=1) - relativedelta(days=(today + relativedelta(months=1)).day))
    inicio = str((today - relativedelta(months=2)).replace(day=1))
week = today.isocalendar()[1]
week_max = week-1
week_min = week-5
month = fin[:-3]

# Competencias
comp = descarga('1d4cMi-V04c80Dpen4S70Q7zYqqho3ZBIVctlRIBLwiw','Competencias')
comp_type = {}
for i in comp['Country'].unique():
    comp_type[i] = comp[comp['Country'] == i]['Tipo'].unique()[0]

###########################################################
#
#       QUERIES
#
###########################################################

print('--------------------------------------------')
print('Arranca Queries')

q_crm = '''WITH wallet_table AS (
    SELECT DISTINCT wa.order_id AS order_id,
           d.campaignCode AS campaign,
           d.amount AS amount
    FROM `peya-bi-tools-pro.il_wallet.fact_wallet_attributions` AS wa,
    UNNEST (attributionDetails) AS d
    WHERE wa.operation = 'Purchase'),
    wc_table AS (
    SELECT cn.country_name AS country,
           c.city_name AS city,
           a.area_name AS area,
           o.registered_date AS fecha,
           wa.campaign AS campaign,
           COUNT(DISTINCT o.order_id) AS orders,
           COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS acq,
           SUM(CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN wa.amount / ce.rate_us ELSE 0 END) AS amount,
           SUM(CASE WHEN bi.payment_mode != 'TOTAL_AMOUNT' THEN wa.amount / ce.rate_us ELSE 0 END) AS amount_shopper
    FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
    INNER JOIN `peya-bi-tools-pro.il_core.fact_order_funding` AS w ON o.order_id = w.order_id AND w.type IN (25,35)
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON o.restaurant.id = p.partner_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_billing_info` AS bi ON p.billingInfo.billing_info_id = bi.billing_info_id
    LEFT JOIN wallet_table AS wa ON o.order_id = wa.order_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
    LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce ON cn.currency_id = ce.currency_id AND DATE_TRUNC(o.registered_date,MONTH) = ce.currency_exchange_date
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
    WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
          AND o.order_status = 'CONFIRMED'
    GROUP BY 1,2,3,4,5)
SELECT wct.country AS Country,
       FORMAT_DATE('%Y-%m',wct.fecha) AS Month,
       EXTRACT(WEEK(MONDAY) FROM wct.fecha) AS Week,
       IFNULL(wct.city,'-') AS City,
       IFNULL(wct.area,'-') AS Area,
       wct.campaign AS Campaign,
       'WALLET' AS Benefit,
       SUM(wct.orders) AS Orders_CRM,
       SUM(wct.acq) AS Acq_CRM,
       SUM(wct.amount) AS Amount,
       SUM(wct.amount_shopper) AS Amount_Shopper
FROM wc_table AS wct
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT cn.country_name AS Country,
       FORMAT_DATE('%Y-%m',o.registered_date) AS Month,
       EXTRACT(WEEK(MONDAY) FROM o.registered_date) AS Week,
       IFNULL(c.city_name,'-') AS City,
       IFNULL(a.area_name,'-') AS Area,
       tca.talon_campaign_name AS Campaign,
       'TALON COUPON' AS Benefit,
       COUNT(DISTINCT o.order_id) AS Orders_CRM,
       COUNT(DISTINCT CASE WHEN o.is_acquisition = 1 THEN o.order_id ELSE NULL END) AS Acq_CRM,
       SUM(CASE WHEN bi.payment_mode = 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS Amount,
       SUM(CASE WHEN bi.payment_mode != 'TOTAL_AMOUNT' THEN tc.coupon_used_amount / ce.rate_us ELSE 0 END) AS Amount_Shopper
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
INNER JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` AS tc ON o.order_id = tc.order_id
LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` AS tca ON tc.talon_campaign_id = tca.talon_campaign_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_currency_exchange` AS ce ON cn.currency_id = ce.currency_id AND DATE_TRUNC(o.registered_date,MONTH) = ce.currency_exchange_date
LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS p ON o.restaurant.id = p.partner_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_billing_info` AS bi ON p.billingInfo.billing_info_id = bi.billing_info_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS c ON o.city.city_id = c.city_id
LEFT JOIN `peya-bi-tools-pro.il_core.dim_area` AS a ON o.address.area.id = a.area_id
WHERE o.registered_date BETWEEN DATE('{0}') AND DATE('{1}')
      AND o.order_status = 'CONFIRMED'
GROUP BY 1,2,3,4,5,6'''.format(inicio,fin)

# Descargo la data
bq_crm = pd.io.gbq.read_gbq(q_crm, project_id='peya-argentina', dialect='standard')

# Copio la data
crm = bq_crm.copy()

print('Finalizo Queries')

###########################################################
#
#       TRABAJO
#
###########################################################

print('--------------------------------------------')
print('Arranca Trabajo')

### TRABAJO

# Trabajo CRM
val = [i for i in crm.columns if i not in ['Country','Month','Week','City','Area','Campaign','Benefit']]
crm[val] = crm[val].astype(float)
col = ['Country','City','Area','Campaign','Benefit']
crm[col] = crm[col].apply(lambda x: x.astype(str).str.upper())
crm['Campaign'] = crm['Campaign'].str.replace(' ', '')

# Marco las campañas a filtrar
crm['Filtrar'] = crm['Campaign'].apply(filtrar_cam)
crm = crm[crm['Filtrar'] == 'No'].copy()
# Trabajo CRM
crm['Type'] = crm['Campaign'].apply(lambda x: filtros(x,'TIPOS'))
crm['Type Wallet'] = crm.apply(lambda x: filtros(x['Campaign'],'TIPOS',x['Benefit']),axis=1)
crm['Segment'] = crm['Campaign'].apply(lambda x: filtros(x,'SEGMENTOS'))
crm['Campaña'] = crm['Campaign'].apply(lambda x: filtros(x,'CAMPAIGNS'))
crm['Budget'] = crm['Campaign'].apply(lambda x: filtros(x,'BUDGET'))
crm['Trial'] = crm['Campaign'].apply(lambda x: filtros(x,'TRIAL'))
# Coloco el IVA
crm = func_iva(crm)
# Saco el IVA
used = 'Amount'
used_shopper = 'Amount_Shopper'
crm['Final'] = crm[used_shopper] / (1 + crm['IVA']) * crm['Extra'] + crm[used]

# Coloco el tipo de Competencia
crm['Competitive Type'] = crm['Country'].apply(lambda x: comp_type[x] if x in list(comp_type.keys()) else '-')
crm['Union'] = crm.apply(lambda x: str(x['Country'])+'-'+str(x['City'])+'-'+str(x['Area']) if x['Competitive Type'] == 'CITY-AREA' else str(x['Country'])+'-'+str(x['City'])+'-',axis=1)
# Hago un merge con la competencia
crm = crm.merge(comp[['Union','Competencia']],on=['Union'],how='left')
crm['Competencia'].replace([np.nan,np.inf,-np.inf],'LOW',inplace=True)
# Marco pais sin Competencia
crm['Pais Competido'] = crm['Country'].apply(lambda x: 'SI' if x in list(comp_type.keys()) else 'NO')

# Coloco la division entre Active/Churn/Reactive
active = ['WELCOME',"'7-15"]
churn = ['15-30']
reactive = ["'+30",'30-90','90-180','180']
crm['RMO Idea'] = '-'
crm['RMO Idea'] = crm.apply(lambda x: 'ACTIVE USERS' if x['Segment'] in active else x['RMO Idea'],axis=1)
crm['RMO Idea'] = crm.apply(lambda x: 'CHURN PREVENTION' if x['Segment'] in churn else x['RMO Idea'],axis=1)
crm['RMO Idea'] = crm.apply(lambda x: 'REACTIVATION' if x['Segment'] in reactive else x['RMO Idea'],axis=1)

# Armo la PT Final
index = ['Country','Pais Competido','Month','Week','Competencia','Budget','Type','Type Wallet','Segment','Campaña','Trial','RMO Idea']
values = ['Orders_CRM','Acq_CRM','Final']
final = crm.pivot_table(index=index,values=values,aggfunc='sum',fill_value=0).reset_index()
# Ordeno el resultado
final.sort_values(['Country','Month','Week','Competencia','Budget','Type','Type Wallet','Segment','Campaña','RMO Idea'],inplace=True)

# Creo los RR
final['Orders RR'] = final.apply(lambda x: round(x['Orders_CRM'] / rr,0) if x['Month'] == month else x['Orders_CRM'],axis=1)
final['Acq RR'] = final.apply(lambda x: round(x['Acq_CRM'] / rr,0) if x['Month'] == month else x['Acq_CRM'],axis=1)
final['Final RR'] = final.apply(lambda x: round(x['Final'] / rr,0) if x['Month'] == month else x['Final'],axis=1)
final[str(today)] = final['Month'].apply(lambda x: rr if x == month else 1)
final.replace([np.nan,np.inf,-np.inf],0,inplace=True)

# Filtro las Budget NONE
final = final[final['Budget'] != 'NONE'].copy()

# Coloco las marcas de Semanas
final['Week Type'] = 'Current'
final['Week Type'] = final.apply(lambda x: 'Last 5 Complete' if week_min <= x['Week'] <= week_max else x['Week Type'],axis=1)
final['Week Type'] = final.apply(lambda x: 'Before' if x['Week'] < week_min else x['Week Type'],axis=1)

# Carga Final
log = carga(final,'1Y5SR_oFtBPJzkpz1r944Iv9UoRutW7fg7kvqYGG5WJc','Crudo','Crudo Gasto por Zonas',log)

print('Finaliza Trabajo')

print('Finalizo GASTO POR ZONAS')

###############################################################################
#                                                                             #
#                                   LOG                                       #
#                                                                             #
###############################################################################

print('--------------------------------------------')
print('Arranca LOG')

carga(log,'10VJBek_8UvxOiEoY4u7tICk2pLJZ9HZ6zfOq81GY20s','Dashboard Paises')

print('Finalizo LOG')