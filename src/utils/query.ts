export const CASE_WHEN_NOME_INTERNO_CAMPANHA = `CASE
        WHEN SIZE(SPLIT(campaign_name, '_')) = 4 THEN 
            INITCAP(REPLACE(SPLIT(campaign_name, '_')[2],'-',' '))
        WHEN LOWER(campaign_name) like '%teste%' AND LOWER(campaign_name) NOT like '%testeab%' AND LOWER(campaign_name) NOT like '%teste ab%' AND LOWER(campaign_name) NOT like '%teste a/b%' AND LOWER(campaign_name) NOT LIKE '%testea/b%' THEN 'Teste de Plataforma (DESCONSIDERAR)'
        WHEN data_minima <= '2023-01-01' THEN 'Antiga'
        WHEN LOWER(campaign_name) like 'Ações_SecomVc_2024_Dezembro%' OR LOWER(campaign_name) like 'ações_secomvc_2024_dezembro%' THEN 'Ações de Internet'
        WHEN LOWER(campaign_name) like '%escola%' THEN 'Segurança nas Escolas'
        WHEN LOWER(campaign_name) like '%pac%' AND LOWER(campaign_name) like '%comp ii%' THEN 'PAC Regional'
        WHEN LOWER(campaign_name) like '%pac%' AND LOWER(campaign_name) like '%comp i%' THEN 'PAC Nacional'
        WHEN LOWER(campaign_name) like '%aceleração do crescimento%' AND LOWER(campaign_name) like '%programa%' AND LOWER(campaign_name) like '%novo%' THEN 'PAC Nacional'
        WHEN LOWER(campaign_name) like '%fe no brasil%' OR LOWER(campaign_name) like '%fé no brasil%' THEN 'Fé no Brasil'
        WHEN LOWER(campaign_name) like '%abril indigena%' OR LOWER(campaign_name) like '%abril indígena%' THEN 'Abril Indígena'
        WHEN LOWER(campaign_name) like '%comunica%' AND LOWER(campaign_name) like '%br%' THEN 'Comunica BR'
        WHEN LOWER(campaign_name) like '%motoristas%' AND (LOWER(campaign_name) like '%aplicativo%' OR LOWER(campaign_name) like '%pl%') THEN 'PL - Motoristas de App'
        WHEN LOWER(campaign_name) like '%desastre climático%' AND LOWER(campaign_name) like '%rio grande do sul%' THEN 'Desastre Climático - RS'
        WHEN LOWER(campaign_name) like '%ação emergencial%' AND LOWER(campaign_name) like '%rio grande do sul%' THEN 'Ação Emergencial - RS'
        WHEN (LOWER(campaign_name) like '%incêndio%' OR LOWER(campaign_name) like '%queimadas%') AND LOWER(campaign_name) like '%amazônia%' THEN 'Incêndios - Bioma Amazônia'
        WHEN (LOWER(campaign_name) like '%incêndio%' OR LOWER(campaign_name) like '%queimadas%') AND LOWER(campaign_name) like '%pantanal%' THEN 'Incêndios - Bioma Pantanal'
        WHEN LOWER(campaign_name) like '%enem%' THEN 'ENEM'
        WHEN LOWER(campaign_name) like '%g20%' OR LOWER(campaign_name) like '%g 20%' THEN 'G20'
        WHEN LOWER(campaign_name) like '%fim de ano%' THEN CONCAT('Fim de Ano - ', IF(MONTH(data_minima) = 1, YEAR(data_minima)-1, YEAR(data_minima)))
        WHEN LOWER(campaign_name) like '%desenrola%' THEN 'Desenrola'
        WHEN (LOWER(campaign_name) like '%salão%' OR LOWER(campaign_name) like '%salao%') AND LOWER(campaign_name) like '%turismo%' THEN CONCAT('Salão do Turismo ', YEAR(data_minima))
        WHEN (LOWER(campaign_name) like '%balanco%' OR LOWER(campaign_name) like '%balanço%') THEN CONCAT('Balanço ',IF(MONTH(data_minima) = 1, YEAR(data_minima)-1, YEAR(data_minima)))
        WHEN (LOWER(campaign_name) like '%acoes%' OR LOWER(campaign_name) like '%acões%' OR LOWER(campaign_name) like '%açoes%' OR LOWER(campaign_name) like '%ações%') AND LOWER(campaign_name) like '%internet%' THEN 'Ações de Internet'
        WHEN LOWER(campaign_name) like '%conselhos%' AND LOWER(campaign_name) like '%tutelares%' THEN 'Conselhos Tutelares'
        WHEN LOWER(campaign_name) like '%carteira%' AND LOWER(campaign_name) like '%nova%' AND LOWER(campaign_name) like '%identidade nacional%' THEN 'Nova ID Nacional'
        WHEN (LOWER(campaign_name) like '%pe%' OR LOWER(campaign_name) like '%pé%') AND LOWER(campaign_name) like '%meia%' THEN 'Pé de Meia'
        WHEN LOWER(campaign_name) like '%combateanoticiasfalsas%' THEN 'Combate Fake News'
        WHEN LOWER(campaign_name) like '%incentivo%' AND LOWER(campaign_name) like '%futebol%' AND LOWER(campaign_name) like '%feminino%'  THEN 'Futebol Feminino'
        WHEN LOWER(campaign_name) like '%abuso%' AND LOWER(campaign_name) like '%crianças%' AND LOWER(campaign_name) like '%adolescentes%' THEN 'Combate Abuso Infantil'
        WHEN (LOWER(campaign_name) like '%consciencia%' OR LOWER(campaign_name) like '%consciência%') AND LOWER(campaign_name) like '%negra%' THEN CONCAT('Consciência Negra - ', YEAR(data_minima))
        WHEN (LOWER(campaign_name) like '%escravidao%' OR LOWER(campaign_name) like '%escravidão%') THEN 'Combate à Escravidão'
        WHEN (campaign_name like '%7%' OR LOWER(campaign_name) like '%sete%') AND LOWER(campaign_name) like '%setembro%' THEN CONCAT("7 de Setembro - ", YEAR(data_minima))
        WHEN LOWER(campaign_name) like '%canalgov%' THEN 'Canal GOV'
        WHEN LOWER(campaign_name) like '%micbr%' OR LOWER(campaign_name) like '%mercado das industrias critivas do brasil%' OR LOWER(campaign_name) like '%mercado das indústrias critivas do brasil%' THEN "MICBR"
        WHEN LOWER(campaign_name) like '%gripe%' AND (LOWER(campaign_name) like '%aviaria%' OR LOWER(campaign_name) like '%aviária%') AND LOWER(campaign_name) like '%nsb%' THEN 'Gripe Aviária - MMA'
        WHEN LOWER(campaign_name) like '%gripe%' AND (LOWER(campaign_name) like '%aviaria%' OR LOWER(campaign_name) like '%aviária%') THEN 'Gripe Aviária - MAP'
        WHEN LOWER(campaign_name) like '%plano%' AND LOWER(campaign_name) like '%safra%' THEN 'Plano Safra'
        WHEN (LOWER(campaign_name) like '%cupula%' AND LOWER(campaign_name) like '%amazonia%') OR (LOWER(campaign_name) like '%cúpula%' AND LOWER(campaign_name) like '%amazônia%') THEN 'Cúpula da Amazônia'
        WHEN LOWER(campaign_name) like '%combate%' AND  LOWER(campaign_name) like '%racismo%' THEN 'Combate ao Racismo'
        WHEN LOWER(campaign_name) like '%lei%' AND LOWER(campaign_name) like '%paulo gustavo%' THEN 'Lei Paulo Gustavo'
        WHEN LOWER(campaign_name) like '%yanomami%' THEN 'Yanomami'
        WHEN LOWER(campaign_name) like '%desastre%' AND LOWER(campaign_name) like '%litoral paulista%' THEN 'Desastre Litoral Paulista'
        WHEN LOWER(campaign_name) like '%dia%' AND LOWER(campaign_name) like '%internacional%' AND LOWER(campaign_name) like '%mulher%' THEN CONCAT('Dia Internacional da Mulher - ',YEAR(data_minima))
        WHEN LOWER(campaign_name) like '%estiagem%' AND (LOWER(campaign_name) like '%rio grande do sul%' OR LOWER(campaign_name) like '%rs%') THEN CONCAT('Estiagem RS - ',YEAR(data_minima))
        WHEN (LOWER(campaign_name) like '%serviço%' OR LOWER(campaign_name) like '%servico%') AND LOWER(campaign_name) like '%militar%' THEN CONCAT('Serviço Militar - ', YEAR(data_minima))
        WHEN LOWER(campaign_name) like '%posicionamento%' AND YEAR(data_minima) = '2023' THEN 'Posicionamento 100 Dias - 2023'
        WHEN LOWER(campaign_name) like '%regional nordeste_ 2023%' THEN 'Regional Nordeste 2023'
        WHEN LOWER(campaign_name) like '%segurança%' AND LOWER(campaign_name) like '%pública%' THEN CONCAT('Segurança Pública - ', YEAR(data_minima))
        WHEN (LOWER(campaign_name) like '%acao%' OR LOWER(campaign_name) like '%ação%' OR LOWER(campaign_name) like '%ações%' OR LOWER(campaign_name) like '%acoes%') AND LOWER(campaign_name) like '%oportunidade%' THEN 'Agenda Positiva'
        WHEN LOWER(campaign_name) like '%ligue%' AND LOWER(campaign_name) like '%180%' THEN 'Ligue 180'
        WHEN LOWER(campaign_name) like '%respeito%' AND LOWER(campaign_name) like '%no%' AND LOWER(campaign_name) like '%carnaval%' THEN 'Respeito No Carnaval'
        WHEN LOWER(campaign_name) like '%nova cin%' OR LOWER(campaign_name) like '%cin%' THEN 'Nova carteira de Identidade'
        WHEN LOWER(campaign_name) like '%cúpula%' AND LOWER(campaign_name) like '%amazônia%' THEN 'Cúpula da Amazônia'
        WHEN LOWER(campaign_name) like '%agosto%' AND (LOWER(campaign_name) like '%lilas%' OR LOWER(campaign_name) like '%lilás%') THEN CONCAT('Agosto Lilás - ', YEAR(data_minima))
        ELSE 'Campanha não identificada'
    END AS Nome_Interno_Campanha`