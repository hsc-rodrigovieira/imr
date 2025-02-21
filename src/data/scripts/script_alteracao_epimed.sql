SELECT First_Value(a.cd_atendimento) OVER (ORDER BY a.cd_atendimento) cd_atendimento_real  
FROM atendime a  
WHERE 1=1
  AND a.cd_paciente = :cd_paciente
  AND a.cd_atendimento > :cd_atendimento
  AND a.tp_atendimento = 'I'
  AND Trunc(a.dt_atendimento) BETWEEN To_Date(:dt_ini,'dd/mm/yyyy') AND To_Date(:dt_ini,'dd/mm/yyyy')+1