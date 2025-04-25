SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '121490150' AS address_src_question_cid,
    'home_address_01' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,  
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_121490150_D_255248624 AS street_num,
    D_121490150_D_945532934 AS street_name,
    D_121490150_D_469838242 AS apartment_num,
    COALESCE(D_121490150_D_303500597, D_920576363_D_725583683) AS city,
    COALESCE(D_121490150_D_195068098, D_920576363_D_917021073) AS state,
    COALESCE(D_121490150_D_202784871, D_920576363_D_970000442) AS zip_code,
    COALESCE(D_121490150_D_831127170, D_920576363_D_500100435) AS country,
    D_804504024_D_105043152 AS cross_street_1,
    D_804504024_D_543135391 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '574985342' AS address_src_question_cid,
    'home_address_02' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_574985342_D_985267931 AS street_num,
    D_574985342_D_111275683 AS street_name,
    D_574985342_D_129226572 AS apartment_num,
    COALESCE(D_574985342_D_536516743, D_444145120_D_288498031) AS city,
    COALESCE(D_574985342_D_283900560, D_444145120_D_195845897) AS state,
    COALESCE(D_574985342_D_467947502, D_444145120_D_936129960) AS zip_code,
    COALESCE(D_574985342_D_368486703, D_444145120_D_924583345) AS country,
    D_398762737_D_553357862 AS cross_street_1,
    D_398762737_D_474541595 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '828086036' AS address_src_question_cid,
    'home_address_03' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_828086036_D_875706715 AS street_num,
    D_828086036_D_645150792 AS street_name,
    D_828086036_D_585360350 AS apartment_num,
    COALESCE(D_828086036_D_588699608, D_752101258_D_648495207) AS city,
    COALESCE(D_828086036_D_544692849, D_752101258_D_300933270) AS state,
    COALESCE(D_828086036_D_335435992, D_752101258_D_649864362) AS zip_code,
    COALESCE(D_828086036_D_337088272, D_752101258_D_723108194) AS country,
    D_961572487_D_469679476 AS cross_street_1,
    D_961572487_D_216954796 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '680046149' AS address_src_question_cid,
    'home_address_04' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_680046149_D_130174162 AS street_num,
    D_680046149_D_563832508 AS street_name,
    D_680046149_D_817839081 AS apartment_num,
    COALESCE(D_680046149_D_930511603, D_879180101_D_931999203) AS city,
    COALESCE(D_680046149_D_756548442, D_879180101_D_486511102) AS state,
    COALESCE(D_680046149_D_455968200, D_879180101_D_267027102) AS zip_code,
    COALESCE(D_680046149_D_728704613, D_879180101_D_734345879) AS country,
    D_746604821_D_423713680 AS cross_street_1,
    D_746604821_D_555767576 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '274189667' AS address_src_question_cid,
    'home_address_05' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_274189667_D_838725845 AS street_num,
    D_274189667_D_565515774 AS street_name,
    D_274189667_D_848348504 AS apartment_num,
    COALESCE(D_274189667_D_742177990, D_212343294_D_445867902) AS city,
    COALESCE(D_274189667_D_843508307, D_212343294_D_348049244) AS state,
    COALESCE(D_274189667_D_554901696, D_212343294_D_684217044) AS zip_code,
    COALESCE(D_274189667_D_819429013, D_212343294_D_600319581) AS country,
    D_298296694_D_915527263 AS cross_street_1,
    D_298296694_D_325919807 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '113930886' AS address_src_question_cid,
    'home_address_06' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_113930886_D_306805272 AS street_num,
    D_113930886_D_819844467 AS street_name,
    D_113930886_D_164233037 AS apartment_num,
    COALESCE(D_113930886_D_418702418, D_255474241_D_218334768) AS city,
    COALESCE(D_113930886_D_101219440, D_255474241_D_394294282) AS state,
    COALESCE(D_113930886_D_127963610, D_255474241_D_803526907) AS zip_code,
    COALESCE(D_113930886_D_882731998, D_255474241_D_941168091) AS country,
    D_205492848_D_756458580 AS cross_street_1,
    D_205492848_D_481599610 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '809728747' AS address_src_question_cid,
    'home_address_07' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_809728747_D_351559015 AS street_num,
    D_809728747_D_903490632 AS street_name,
    D_809728747_D_906119853 AS apartment_num,
    COALESCE(D_809728747_D_703944664, D_201906316_D_476697171) AS city,
    COALESCE(D_809728747_D_390463636, D_201906316_D_605344820) AS state,
    COALESCE(D_809728747_D_256790385, D_201906316_D_814644814) AS zip_code,
    COALESCE(D_809728747_D_915222355, D_201906316_D_627992821) AS country,
    D_581231591_D_732107715 AS cross_street_1,
    D_581231591_D_803219073 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '539057792' AS address_src_question_cid,
    'home_address_08' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_539057792_D_893639464 AS street_num,
    D_539057792_D_438475588 AS street_name,
    D_539057792_D_194165243 AS apartment_num,
    COALESCE(D_539057792_D_744290061, D_864213677_D_280877371) AS city,
    COALESCE(D_539057792_D_516936572, D_864213677_D_463064782) AS state,
    COALESCE(D_539057792_D_673034401, D_864213677_D_865310914) AS zip_code,
    COALESCE(D_539057792_D_489019597, D_864213677_D_900377581) AS country,
    D_123104885_D_707276214 AS cross_street_1,
    D_123104885_D_462701424 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '537011756' AS address_src_question_cid,
    'home_address_09' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_537011756_D_988266183 AS street_num,
    D_537011756_D_709374950 AS street_name,
    D_537011756_D_853931010 AS apartment_num,
    COALESCE(D_537011756_D_351559021, D_964853797_D_548773158) AS city,
    COALESCE(D_537011756_D_290370013, D_964853797_D_487043303) AS state,
    COALESCE(D_537011756_D_453691095, D_964853797_D_659122266) AS zip_code,
    COALESCE(D_537011756_D_202104231, D_964853797_D_388427546) AS country,
    D_890661849_D_174111872 AS cross_street_1,
    D_890661849_D_735022625 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '171937884' AS address_src_question_cid,
    'home_address_10' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_171937884_D_264707783 AS street_num,
    D_171937884_D_666011940 AS street_name,
    D_171937884_D_981594981 AS apartment_num,
    COALESCE(D_171937884_D_282089547, D_787064287_D_891573875) AS city,
    COALESCE(D_171937884_D_612617245, D_787064287_D_862255177) AS state,
    COALESCE(D_171937884_D_674024553, D_787064287_D_972332937) AS zip_code,
    COALESCE(D_171937884_D_886247195, D_787064287_D_429200007) AS country,
    D_902193418_D_633590687 AS cross_street_1,
    D_902193418_D_857265979 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '828766803' AS address_src_question_cid,
    'home_address_11' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_828766803_D_622968789 AS street_num,
    D_828766803_D_696874548 AS street_name,
    D_828766803_D_450630128 AS apartment_num,
    COALESCE(D_828766803_D_309461541, D_878688378_D_706592013) AS city,
    COALESCE(D_828766803_D_789637860, D_878688378_D_585473282) AS state,
    COALESCE(D_828766803_D_795253129, D_878688378_D_814137809) AS zip_code,
    COALESCE(D_828766803_D_780298998, D_878688378_D_876521406) AS country,
    D_440597740_D_573998459 AS cross_street_1,
    D_440597740_D_760197341 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '376408004' AS address_src_question_cid,
    'seasonal_address_01' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_376408004_D_234037089 AS street_num,
    D_376408004_D_416862112 AS street_name,
    D_376408004_D_671149035 AS apartment_num,
    COALESCE(D_376408004_D_556576930, D_173413183_D_416620941) AS city,
    COALESCE(D_376408004_D_304326324, D_173413183_D_915859406) AS state,
    COALESCE(D_376408004_D_812433386, D_173413183_D_354833686) AS zip_code,
    COALESCE(D_376408004_D_477319994, D_173413183_D_661148931) AS country,
    D_200086909_D_351319555 AS cross_street_1,
    D_200086909_D_154163153 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '279093430' AS address_src_question_cid,
    'seasonal_address_02' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_279093430_D_476938134 AS street_num,
    D_279093430_D_561635035 AS street_name,
    D_279093430_D_134210521 AS apartment_num,
    COALESCE(D_279093430_D_715370929, D_657986901_D_726739712) AS city,
    COALESCE(D_279093430_D_109991481, D_657986901_D_149514187) AS state,
    COALESCE(D_279093430_D_494380686, D_657986901_D_845446624) AS zip_code,
    COALESCE(D_279093430_D_440796912, D_657986901_D_677739650) AS country,
    D_509526051_D_542763783 AS cross_street_1,
    D_509526051_D_351069956 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '143927994' AS address_src_question_cid,
    'seasonal_address_03' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_143927994_D_119483547 AS street_num,
    D_143927994_D_194338739 AS street_name,
    D_143927994_D_387077376 AS apartment_num,
    COALESCE(D_143927994_D_113352592, D_564684946_D_148846635) AS city,
    COALESCE(D_143927994_D_768114466, D_564684946_D_192663941) AS state,
    COALESCE(D_143927994_D_938180781, D_564684946_D_245044197) AS zip_code,
    COALESCE(D_143927994_D_733365745, D_564684946_D_261025083) AS country,
    D_370121390_D_580185896 AS cross_street_1,
    D_370121390_D_599607007 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '935378391' AS address_src_question_cid,
    'seasonal_address_04' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_935378391_D_785588454 AS street_num,
    D_935378391_D_419659205 AS street_name,
    D_935378391_D_653181757 AS apartment_num,
    COALESCE(D_935378391_D_733619119, D_558981691_D_571926996) AS city,
    COALESCE(D_935378391_D_843680322, D_558981691_D_645589113) AS state,
    COALESCE(D_935378391_D_716343828, D_558981691_D_701056236) AS zip_code,
    COALESCE(D_935378391_D_497260033, D_558981691_D_398249766) AS country,
    D_192184336_D_117544868 AS cross_street_1,
    D_192184336_D_868650023 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '320166033' AS address_src_question_cid,
    'seasonal_address_05' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_320166033_D_275244758 AS street_num,
    D_320166033_D_688216428 AS street_name,
    D_320166033_D_843674851 AS apartment_num,
    COALESCE(D_320166033_D_570311888, D_194944818_D_101804763) AS city,
    COALESCE(D_320166033_D_557852952, D_194944818_D_502068619) AS state,
    COALESCE(D_320166033_D_970217879, D_194944818_D_787391994) AS zip_code,
    COALESCE(D_320166033_D_452103273, D_194944818_D_540340377) AS country,
    D_763354979_D_677922318 AS cross_street_1,
    D_763354979_D_424347938 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '383535171' AS address_src_question_cid,
    'seasonal_address_06' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_383535171_D_666445636 AS street_num,
    D_383535171_D_398622449 AS street_name,
    D_383535171_D_521925072 AS apartment_num,
    COALESCE(D_383535171_D_888514303, D_508587741_D_686611963) AS city,
    COALESCE(D_383535171_D_905002640, D_508587741_D_900950849) AS state,
    COALESCE(D_383535171_D_687407917, D_508587741_D_103689435) AS zip_code,
    COALESCE(D_383535171_D_932828568, D_508587741_D_659457234) AS country,
    D_355179190_D_115195973 AS cross_street_1,
    D_355179190_D_706861475 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '133566757' AS address_src_question_cid,
    'seasonal_address_07' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_133566757_D_199489170 AS street_num,
    D_133566757_D_605155921 AS street_name,
    D_133566757_D_300476868 AS apartment_num,
    COALESCE(D_133566757_D_384403974, D_293954660_D_860984191) AS city,
    COALESCE(D_133566757_D_585153023, D_293954660_D_892150843) AS state,
    COALESCE(D_133566757_D_248996395, D_293954660_D_230376384) AS zip_code,
    COALESCE(D_133566757_D_525778327, D_293954660_D_526462982) AS country,
    D_851731394_D_993557817 AS cross_street_1,
    D_851731394_D_110516520 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '509553290' AS address_src_question_cid,
    'seasonal_address_08' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_509553290_D_295693777 AS street_num,
    D_509553290_D_646099557 AS street_name,
    D_509553290_D_886284650 AS apartment_num,
    COALESCE(D_509553290_D_367684056, D_268612977_D_599753334) AS city,
    COALESCE(D_509553290_D_389478638, D_268612977_D_467126157) AS state,
    COALESCE(D_509553290_D_949478044, D_268612977_D_421779583) AS zip_code,
    COALESCE(D_509553290_D_298170847, D_268612977_D_587765197) AS country,
    D_172669345_D_520630754 AS cross_street_1,
    D_172669345_D_142318726 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '239279719' AS address_src_question_cid,
    'seasonal_address_09' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_239279719_D_143093472 AS street_num,
    D_239279719_D_746619983 AS street_name,
    D_239279719_D_911964974 AS apartment_num,
    COALESCE(D_239279719_D_711881258, D_216096388_D_450433102) AS city,
    COALESCE(D_239279719_D_390941579, D_216096388_D_181005197) AS state,
    COALESCE(D_239279719_D_737885885, D_216096388_D_855530921) AS zip_code,
    COALESCE(D_239279719_D_603853574, D_216096388_D_589689090) AS country,
    D_921998144_D_872527709 AS cross_street_1,
    D_921998144_D_686647703 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '778711683' AS address_src_question_cid,
    'seasonal_address_10' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_778711683_D_117703279 AS street_num,
    D_778711683_D_734790700 AS street_name,
    D_778711683_D_278164536 AS apartment_num,
    COALESCE(D_778711683_D_160188014, D_757983656_D_983038259) AS city,
    COALESCE(D_778711683_D_596751155, D_757983656_D_313586037) AS state,
    COALESCE(D_778711683_D_624226136, D_757983656_D_158186064) AS zip_code,
    COALESCE(D_778711683_D_807127029, D_757983656_D_274940131) AS country,
    D_670316988_D_306092529 AS cross_street_1,
    D_670316988_D_258544530 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '632533534' AS address_src_question_cid,
    'childhood_address_01' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_632533534_D_284547539 AS street_num,
    D_632533534_D_802585033 AS street_name,
    D_632533534_D_746533238 AS apartment_num,
    COALESCE(D_632533534_D_128827522, D_264797252_D_890792569) AS city,
    COALESCE(D_632533534_D_439447560, D_264797252_D_451394598) AS state,
    COALESCE(D_632533534_D_286781627, D_264797252_D_984908796) AS zip_code,
    COALESCE(D_632533534_D_733929451, D_264797252_D_847327251) AS country,
    D_469914719_D_952124199 AS cross_street_1,
    D_469914719_D_204186397 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '596318751' AS address_src_question_cid,
    'current_work_address_01' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_596318751_D_493984171 AS street_num,
    D_596318751_D_253017624 AS street_name,
    D_596318751_D_404141282 AS apartment_num,
    COALESCE(D_596318751_D_959804472, D_263588196_D_583500714) AS city,
    COALESCE(D_596318751_D_774707280, D_263588196_D_742105146) AS state,
    COALESCE(D_596318751_D_182144476, D_263588196_D_101341673) AS zip_code,
    COALESCE(D_596318751_D_294634899, D_263588196_D_237204853) AS country,
    D_845811202_D_510435329 AS cross_street_1,
    D_845811202_D_520264332 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '992180692' AS address_src_question_cid,
    'previous_work_address_01' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_992180692_D_903896611 AS street_num,
    D_992180692_D_855583262 AS street_name,
    D_992180692_D_371588177 AS apartment_num,
    COALESCE(D_992180692_D_962868433, D_350394531_D_652022112) AS city,
    COALESCE(D_992180692_D_108530997, D_350394531_D_730666903) AS state,
    COALESCE(D_992180692_D_110852652, D_350394531_D_168091937) AS zip_code,
    COALESCE(D_992180692_D_867109611, D_350394531_D_132779701) AS country,
    D_733317111_D_584350267 AS cross_street_1,
    D_733317111_D_840147245 AS cross_street_2
FROM @flat_module4

UNION ALL

SELECT
    CAST(Connect_ID AS STRING) AS Connect_ID,
    NULL AS ts_user_profile_updated,
    CURRENT_TIMESTAMP() AS ts_address_delivered,
    '914696832' AS address_src_question_cid,
    'school_address_01' AS address_nickname,
    'module4' AS address_source,
    NULL AS historical_order,
    NULL AS address_line_1,
    NULL AS address_line_2,
    D_914696832_D_970996351 AS street_num,
    D_914696832_D_249657148 AS street_name,
    D_914696832_D_190883115 AS apartment_num,
    COALESCE(D_914696832_D_161170041, D_668887646_D_225725599) AS city,
    COALESCE(D_914696832_D_660217075, D_668887646_D_977086216) AS state,
    COALESCE(D_914696832_D_884494489, D_668887646_D_997041632) AS zip_code,
    COALESCE(D_914696832_D_403679963, D_668887646_D_147113671) AS country,
    D_443679537_D_494271326 AS cross_street_1,
    D_443679537_D_952170182 AS cross_street_2
FROM @flat_module4