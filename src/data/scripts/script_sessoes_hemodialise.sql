SELECT
  Trunc(dt_lancamento,'month') mes,
  Sum(qt_lancamento) qtd 
FROM
( SELECT
    rf.cd_atendimento,
    irf.cd_procedimento,
    dt_lancamento,
    qt_lancamento
  FROM reg_fat rf 
    INNER JOIN itreg_fat irf ON rf.cd_reg_fat = irf.cd_reg_fat
    INNER JOIN procedimento_sus psus ON irf.cd_procedimento = psus.cd_procedimento AND psus.cd_grupo_procedimento = '03' AND psus.cd_sub_grupo_procedimento = '05'
    left JOIN itpre_med ipm ON irf.cd_itmvto = ipm.cd_itpre_med
    left JOIN pre_med pm ON ipm.cd_pre_med = pm.cd_pre_med
  WHERE Trunc(irf.dt_lancamento) BETWEEN To_Date(:dt_ini,'DD/MM/YYYY') AND To_Date(:dt_fim,'DD/MM/YYYY') 
  UNION ALL 
  SELECT
    rf.cd_atendimento,
    pf.cd_pro_fat,
    dt_lancamento,
    qt_lancamento
  FROM reg_fat rf 
    INNER JOIN itreg_fat irf ON rf.cd_reg_fat = irf.cd_reg_fat
    INNER JOIN pro_fat pf ON irf.cd_pro_fat = pf.cd_pro_fat AND pf.cd_gru_pro = 15 AND pf.ds_pro_fat LIKE '%DIALISE%' AND pf.ds_pro_fat NOT LIKE 'I%'
    left JOIN itpre_med ipm ON irf.cd_itmvto = ipm.cd_itpre_med
    left JOIN pre_med pm ON ipm.cd_pre_med = pm.cd_pre_med 
  WHERE Trunc(irf.dt_lancamento) BETWEEN To_Date(:dt_ini,'DD/MM/YYYY') AND To_Date(:dt_fim,'DD/MM/YYYY')
) GROUP BY Trunc(dt_lancamento,'month')