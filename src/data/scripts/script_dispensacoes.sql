SELECT
  base.cd_atendimento
  ,base.ds_unid_int
  ,matmed.tp_classificacao
  ,matmed.cd_produto
  ,matmed.ds_produto
  ,To_Char(matmed.dt_gravacao,'dd/mm/yyyy') dt_consumo
  ,matmed.qt_movimentacao
  ,Nvl(matmed.unid_ref,'N/A') unid_ref
  ,matmed.vl_custo_medio vl_unitario
  ,matmed.qt_movimentacao * matmed.vl_custo_medio vl_total
FROM
( SELECT DISTINCT mov.cd_atendimento, contador.data, ds_unid_int
  FROM (SELECT To_Date(:dt_ini,'DD/MM/YYYY')+LEVEL-1 data FROM dual CONNECT BY LEVEL <= (To_Date(:dt_fim,'DD/MM/YYYY')-To_Date(:dt_ini,'DD/MM/YYYY')+1)) contador -- BASE DE DIAS
  INNER JOIN
  ( SELECT  -- SETOR DOS ATENDIMENTOS DE INTERNACAO
      a.cd_atendimento
      ,l.ds_leito
      ,Decode(ui.cd_unid_int,6,'UTI AD',25,'UTI AD2') ds_unid_int
      ,Decode(a.cd_convenio,1,'SUS','CONV/PART.') tp_conv
      ,To_Date(To_Char(dt_mov_int,'dd/mm/YYYY')||To_Char(hr_mov_int,'hh24:mi:ss'),'dd/mm/YYYY hh24:mi:ss') dh_mov_int
      ,To_Date(To_Char(dt_lib_mov,'dd/mm/YYYY')||To_Char(hr_lib_mov,'hh24:mi:ss'),'dd/mm/YYYY hh24:mi:ss') dh_lib_mov
    FROM mov_int mi
      INNER JOIN leito l ON mi.cd_leito = l.cd_leito
      INNER JOIN unid_int ui ON l.cd_unid_int = ui.cd_unid_int
      INNER JOIN atendime a ON mi.cd_atendimento = a.cd_atendimento
    WHERE tp_mov IN ('O','I')
      AND ui.cd_unid_int IN (6,25)
  ) mov ON contador.data BETWEEN Trunc(mov.dh_mov_int) AND Trunc(Nvl(mov.dh_lib_mov,SYSDATE))
) base
INNER JOIN -- CONSUMO
( -- PRESCRI��O EXAMES DE IMAGEM
  SELECT
    cd_atendimento,tp_classificacao,cd_procedimento cd_produto,ds_exa_rx ds_produto,dt_pedido dt_gravacao,und unid_ref,Count(cd_itped_rx) qt_movimentacao,val vl_custo_medio
  FROM
  ( SELECT DISTINCT
      rx.cd_atendimento
      ,'EXAMES - IMAGEM' tp_classificacao
      ,Nvl(Nvl(erx.cd_procedimento_sia,erx.cd_procedimento_sih),erx.exa_rx_cd_pro_fat) cd_procedimento
      ,erx.ds_exa_rx
      ,Trunc(rx.dt_pedido) dt_pedido
      ,'UNIDADE' und
      ,ipr.cd_itped_rx
      ,Last_Value(vp.vl_total) OVER (PARTITION BY erx.cd_exa_rx ORDER BY vp.dt_vigencia ROWS BETWEEN unbounded preceding AND unbounded following) * .75 val
    FROM ped_rx rx
      INNER JOIN itped_rx ipr ON rx.cd_ped_rx = ipr.cd_ped_rx
      INNER JOIN exa_rx erx ON ipr.cd_exa_rx = erx.cd_exa_rx
      left JOIN val_pro vp ON vp.cd_pro_fat = erx.exa_rx_cd_pro_fat AND Trunc(vp.dt_vigencia) <= Trunc(rx.dt_pedido) AND vp.cd_tab_fat IN (39,3)
    WHERE 1=1
      AND Trunc(rx.dt_pedido) BETWEEN to_date(:dt_ini, 'DD/MM/YYYY') AND to_date(:dt_fim, 'DD/MM/YYYY')
      AND rx.cd_setor IN (161,515)
      AND EXISTS (SELECT 1 FROM itreg_fat irf WHERE irf.cd_itmvto = ipr.cd_itped_rx)
  ) GROUP BY cd_atendimento,tp_classificacao,cd_procedimento,ds_exa_rx,dt_pedido,und,val
  -- FIM EXAMES DE IMAGEM

  UNION ALL

  -- PRESCRI��O EXAMES LABORATORIAIS
  SELECT
    cd_atendimento,tp_classificacao,cd_procedimento,nm_exa_lab,dt_pedido,und,Count(cd_itped_lab) nr_faturado,val
  FROM
  ( SELECT DISTINCT
      pdl.cd_atendimento
      ,'EXAMES - LABORATORIAL' tp_classificacao
      ,Nvl(Nvl(el.cd_procedimento_sia,el.cd_procedimento_sih),el.cd_pro_fat) cd_procedimento
      ,el.nm_exa_lab
      ,Trunc(pdl.dt_pedido) dt_pedido
      ,'UNIDADE' und
      ,ipl.cd_itped_lab
      ,Last_Value(vp.vl_total) OVER (PARTITION BY el.cd_exa_lab ORDER BY vp.dt_vigencia ROWS BETWEEN unbounded preceding AND unbounded following) * .75 val
    FROM ped_lab pdl
      INNER JOIN itped_lab ipl ON pdl.cd_ped_lab = ipl.cd_ped_lab
      INNER JOIN exa_lab el ON ipl.cd_exa_lab = el.cd_exa_lab
      left JOIN val_pro vp ON vp.cd_pro_fat = el.cd_pro_fat AND Trunc(vp.dt_vigencia) <= Trunc(pdl.dt_pedido) AND vp.cd_tab_fat = 21
    WHERE 1=1
      AND Trunc(pdl.dt_pedido) BETWEEN to_date(:dt_ini, 'DD/MM/YYYY') AND to_date(:dt_fim, 'DD/MM/YYYY')
      AND pdl.cd_setor IN (161,515)
      AND EXISTS (SELECT 1 FROM itreg_fat irf WHERE irf.cd_itmvto = ipl.cd_itped_lab)
  ) GROUP BY cd_atendimento,tp_classificacao,cd_procedimento,nm_exa_lab,dt_pedido,und,val
  -- FIM EXAMES LABORATORIAIS

  UNION ALL

  -- PRESCRI��O DE HEMODERIVADOS
  SELECT
    cd_atendimento,tp_classificacao,cd_procedimento_sih,ds_sangue_derivados,dt_pedido,und,Sum(qt_solicitada) nr_faturado,val
  FROM
  ( SELECT DISTINCT
      ss.cd_atendimento
      ,'PROCEDIMENTO - TRANSFUSAO' tp_classificacao
      ,tp.cd_procedimento_sih
      ,sd.ds_sangue_derivados
      ,Trunc(ss.dt_solic_sangue) dt_pedido
      ,'UNIDADE' und
      ,iss.cd_it_solic_sangue
      ,iss.qt_solicitada
      ,Last_Value(vp.vl_total) OVER (PARTITION BY sd.cd_sangue_derivados ORDER BY vp.dt_vigencia ROWS BETWEEN unbounded preceding AND unbounded following) * .75 val
    FROM solic_sangue ss
      INNER JOIN it_solic_sangue iss ON ss.cd_solic_sangue = iss.cd_solic_sangue
      INNER JOIN sangue_derivados sd ON iss.cd_sangue_derivados = sd.cd_sangue_derivados
      INNER JOIN tip_presc tp ON sd.cd_sangue_derivados = tp.cd_sangue_derivados
      left JOIN val_pro vp ON vp.cd_pro_fat = sd.cd_pro_fat AND Trunc(vp.dt_vigencia) <= Trunc(ss.dt_solic_sangue) AND vp.cd_tab_fat = 21
    WHERE 1=1
      AND Trunc(ss.dt_solic_sangue) BETWEEN To_Date(:dt_ini, 'DD/MM/YYYY') AND To_Date(:dt_fim, 'DD/MM/YYYY')
      AND ss.cd_setor IN (161,515)
      AND EXISTS (SELECT 1 FROM itreg_fat irf WHERE irf.cd_itmvto = iss.cd_it_solic_sangue)
  ) GROUP BY cd_atendimento,tp_classificacao,cd_procedimento_sih,ds_sangue_derivados,dt_pedido,und,val
  -- FIM HEMODERIVADOS

  UNION ALL

  -- HEMODIALISE
  SELECT
    rf.cd_atendimento,
    'PROCEDIMENTO - HEMODIALISE',
    irf.cd_procedimento,
    psus.ds_procedimento,
    dt_lancamento,
    'UNIDADE',
    qt_lancamento,
    60 
  FROM reg_fat rf
    INNER JOIN itreg_fat irf ON rf.cd_reg_fat = irf.cd_reg_fat
    INNER JOIN procedimento_sus psus ON irf.cd_procedimento = psus.cd_procedimento AND psus.cd_grupo_procedimento = '03' AND psus.cd_sub_grupo_procedimento = '05'
    left JOIN itpre_med ipm ON irf.cd_itmvto = ipm.cd_itpre_med
    left JOIN pre_med pm ON ipm.cd_pre_med = pm.cd_pre_med
  WHERE 1=1
    AND Trunc(irf.dt_lancamento) BETWEEN To_Date(:dt_ini,'DD/MM/YYYY') AND To_Date(:dt_fim,'DD/MM/YYYY')
    AND irf.cd_setor IN (161,515)
  UNION ALL
  SELECT
    rf.cd_atendimento,
    'PROCEDIMENTO - HEMODIALISE',
    pf.cd_pro_fat,
    pf.ds_pro_fat,
    dt_lancamento,
    'UNIDADE',
    qt_lancamento,
    60
  FROM reg_fat rf
    INNER JOIN itreg_fat irf ON rf.cd_reg_fat = irf.cd_reg_fat
    INNER JOIN pro_fat pf ON irf.cd_pro_fat = pf.cd_pro_fat AND pf.cd_gru_pro = 15 AND pf.ds_pro_fat LIKE '%DIALISE%' AND pf.ds_pro_fat NOT LIKE 'I%' 
    left JOIN itpre_med ipm ON irf.cd_itmvto = ipm.cd_itpre_med
    left JOIN pre_med pm ON ipm.cd_pre_med = pm.cd_pre_med
  WHERE 1=1
    AND Trunc(irf.dt_lancamento) BETWEEN To_Date(:dt_ini,'DD/MM/YYYY') AND To_Date(:dt_fim,'DD/MM/YYYY')
    AND irf.cd_setor IN (161,515)
) matmed ON base.cd_atendimento = matmed.cd_atendimento AND base.data = matmed.dt_gravacao
WHERE qt_movimentacao >= 1 ORDER BY 1,2,To_Date(dt_consumo,'dd/mm/yyyy'),4
;

SELECT
  base.cd_atendimento,base.ds_unid_int,tp_classificacao,tp_mov,cd_produto,ds_produto,dt_gravacao,qt_movimentacao,unid_ref,vl_custo_medio
FROM
( SELECT DISTINCT mov.cd_atendimento, contador.data, mov.ds_unid_int
  FROM (SELECT To_Date(:dt_ini,'DD/MM/YYYY')+LEVEL-1 data FROM dual CONNECT BY LEVEL <= (To_Date(:dt_fim,'DD/MM/YYYY')-To_Date(:dt_ini,'DD/MM/YYYY')+1)) contador
  INNER JOIN
  ( SELECT  -- SETOR DOS ATENDIMENTOS DE INTERNACAO
      a.cd_atendimento
      ,l.ds_leito
      ,Decode(ui.cd_unid_int,6,'UTI AD',25,'UTI AD2') ds_unid_int
      ,Decode(a.cd_convenio,1,'SUS','CONV/PART.') tp_conv
      ,To_Date(To_Char(dt_mov_int,'dd/mm/YYYY')||To_Char(hr_mov_int,'hh24:mi:ss'),'dd/mm/YYYY hh24:mi:ss') dh_mov_int
      ,To_Date(To_Char(dt_lib_mov,'dd/mm/YYYY')||To_Char(hr_lib_mov,'hh24:mi:ss'),'dd/mm/YYYY hh24:mi:ss') dh_lib_mov
    FROM mov_int mi
      INNER JOIN leito l ON mi.cd_leito = l.cd_leito
      INNER JOIN unid_int ui ON l.cd_unid_int = ui.cd_unid_int
      INNER JOIN atendime a ON mi.cd_atendimento = a.cd_atendimento
    WHERE tp_mov IN ('O','I')
      AND ui.cd_unid_int IN (6,25)
  ) mov ON contador.data BETWEEN Trunc(mov.dh_mov_int) AND Trunc(Nvl(mov.dh_lib_mov,SYSDATE))
) base
INNER JOIN
( SELECT DISTINCT
    cd_atendimento
    ,tp_classificacao
    ,Decode(tp_mvto_estoque,'P','SAIDA','C','DEVOLUCAO') tp_mov
    ,cd_produto
    ,ds_produto
    ,CASE
      WHEN tp_mvto_estoque = 'P' THEN dt_gravacao
      WHEN tp_mvto_estoque = 'C' THEN dt_gravacao+.9999
    END dt_gravacao
    ,REPLACE(unid_ref,' ENTRADA','') unid_ref
    ,CASE
      WHEN unidade_mvto <> unidade_padrao THEN Ceil(Sum(qt_movimentacao * vl_fator) OVER (PARTITION BY cd_atendimento,tp_mvto_estoque,cd_produto,dt_gravacao))
      ELSE Ceil(Sum(qt_movimentacao) OVER (PARTITION BY cd_atendimento,tp_mvto_estoque,cd_produto,dt_gravacao))
    END qt_movimentacao
    ,vl_custo_medio
  FROM
  ( SELECT DISTINCT
      me.cd_atendimento
      ,me.cd_mvto_estoque
      ,ime.cd_itmvto_estoque
      ,me.tp_mvto_estoque
      ,Decode(p.cd_especie, 1, 'MEDICAMENTOS'
                          , 2, 'MATERIAL HOSPITALAR'
                          --, 5, 'MATERIAL HOSPITALAR'
                          , 5, 'MATERIAL OPME'
                          --, 7, 'MEDICAMENTOS'
                          , 7, 'COZINHA'
                          ) tp_classificacao
      ,p.cd_produto
      ,p.ds_produto
      ,Trunc(ime.dh_mvto_estoque) dt_gravacao
      ,upc.ds_unidade unid_ref
      ,ime.qt_movimentacao
      ,upm.vl_fator
      ,Last_Value(cmm.vl_custo_medio) OVER (PARTITION BY cmm.cd_produto ORDER BY cmm.dh_custo_medio ROWS BETWEEN unbounded preceding AND unbounded following) vl_custo_medio
      ,upm.cd_uni_pro unidade_mvto
      ,upc.cd_uni_pro unidade_padrao
    FROM mvto_estoque me
      INNER JOIN itmvto_estoque ime ON me.cd_mvto_estoque = ime.cd_mvto_estoque
      INNER JOIN uni_pro upm ON ime.cd_produto = upm.cd_produto AND ime.cd_uni_pro = upm.cd_uni_pro
      INNER JOIN produto p ON ime.cd_produto = p.cd_produto
      INNER JOIN uni_pro upc ON p.cd_produto = upc.cd_produto AND upc.tp_relatorios = 'R'
      left  JOIN custo_medio_mensal cmm ON p.cd_produto = cmm.cd_produto AND Trunc(ime.dh_mvto_estoque,'month') >= Trunc(cmm.dh_custo_medio,'month')
    WHERE 1=1
      AND Trunc(ime.dh_mvto_estoque) BETWEEN to_date(:dt_ini, 'DD/MM/YYYY') AND to_date(:dt_fim, 'DD/MM/YYYY')
      AND me.tp_mvto_estoque IN ( 'P','C' )
      AND me.cd_unid_int IN ( 6,25 )      
      AND p.cd_especie IN ( 1,2,5,7 )
  )
) disp ON base.cd_atendimento = disp.cd_atendimento AND base.data = Trunc(disp.dt_gravacao)
ORDER BY 2,6,4,3 DESC
