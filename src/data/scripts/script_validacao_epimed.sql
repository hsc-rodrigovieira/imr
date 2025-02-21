SELECT
  a.cd_atendimento
  ,CASE
	  WHEN sn_utiliza_nome_social = 'S'               THEN                regexp_replace(Trim(regexp_replace(p.nm_paciente,'(^| )([^ ])([^ ])*','\2')),'\w\Z','') || ' ' || regexp_substr(nm_social_paciente,'(\w+)\Z')
	  WHEN Trim(SubStr(p.nm_paciente, 0,5)) = 'RNII'  THEN 'RN II DE ' || regexp_replace(regexp_replace(Trim(REPLACE(p.nm_paciente,'RNII DE','')), '(^| )([^ ])([^ ])*','\2'),'\w\Z','') || ' ' || regexp_substr(Trim(p.nm_paciente),'(\w+)\Z')
	  WHEN Trim(SubStr(p.nm_paciente, 0,5)) = 'RN II' THEN 'RN II DE ' || regexp_replace(regexp_replace(Trim(REPLACE(p.nm_paciente,'RN II DE','')),'(^| )([^ ])([^ ])*','\2'),'\w\Z','') || ' ' || regexp_substr(Trim(p.nm_paciente),'(\w+)\Z')
	  WHEN Trim(SubStr(p.nm_paciente, 0,4)) = 'RNI'   THEN 'RN I DE '  || regexp_replace(regexp_replace(Trim(REPLACE(p.nm_paciente,'RNI DE','')),  '(^| )([^ ])([^ ])*','\2'),'\w\Z','') || ' ' || regexp_substr(Trim(p.nm_paciente),'(\w+)\Z')
	  WHEN Trim(SubStr(p.nm_paciente, 0,4)) = 'RN I'  THEN 'RN I DE '  || regexp_replace(regexp_replace(Trim(REPLACE(p.nm_paciente,'RN I DE','')), '(^| )([^ ])([^ ])*','\2'),'\w\Z','') || ' ' || regexp_substr(Trim(p.nm_paciente),'(\w+)\Z')
	  WHEN Trim(SubStr(p.nm_paciente, 0,2)) = 'RN'    THEN 'RN DE '    || regexp_replace(regexp_replace(Trim(REPLACE(p.nm_paciente,'RN ','')),     '(^| )([^ ])([^ ])*','\2'),'\w\Z','') || ' ' || regexp_substr(Trim(p.nm_paciente),'(\w+)\Z')
	  ELSE regexp_replace(Trim(regexp_replace(p.nm_paciente,'(^| )([^ ])([^ ])*','\2')),'\w\Z','') || ' ' || regexp_substr(Trim(p.nm_paciente),'(\w+)\Z')
  END pac
  ,a.cd_paciente
  ,To_Char(a.dt_atendimento,'dd/mm/yyyy') dt
  ,Decode(a.tp_atendimento,'A','AMBULATORIAL','U','URGENCIA') tp_atend  
FROM atendime a INNER JOIN paciente p ON a.cd_paciente = p.cd_paciente 
WHERE a.cd_atendimento IN ({:}) AND a.tp_atendimento <> 'I'  
ORDER BY 1