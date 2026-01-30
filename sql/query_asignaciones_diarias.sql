WITH base AS (
    SELECT 
        *,
        CASE 
            WHEN (excluir_vicidial = 1 OR excluir_soul = 1) THEN 1 
            ELSE 0 
        END AS exclusiones_general
    FROM bbdd_cos_bog_colsubsidio_agencia_empleo.tb_asignacion_intermediacion_v2_coalesce
    WHERE periodo = 202601
),
consolidados AS (
    SELECT *,
           MAX(exclusiones_general) OVER(PARTITION BY phone) AS exclusion_total
    FROM base
)
SELECT 
    c.*
FROM consolidados c
LEFT JOIN bbdd_cos_bog_colsubsidio_agencia_empleo.tb_mensajes_reporting r
    ON CONVERT(c.codigo_unico_vacante USING utf8mb4) COLLATE utf8mb4_unicode_ci = 
       CONVERT(r.codigo_unico_vacante USING utf8mb4) COLLATE utf8mb4_unicode_ci
WHERE c.exclusion_total = 0
    AND r.codigo_unico_vacante IS NULL
    AND c.tipo_phone IN ('movil_1')
    AND c.vicidial_calls >= 1
    AND (
        c.tipificacion_mejor_gestion_soul IN ('No contesta', 'Llamada muda')
        OR c.tipificacion_mejor_gestion_soul IS NULL
    )
    AND DATE(c.fecha_asignacion)
      BETWEEN DATE_FORMAT(CURDATE(), '%%Y-%%m-01')
      AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
ORDER BY 
    CASE 
        WHEN c.tipo_phone = 'movil_1' AND (c.vicidial_calls <= 3 OR c.vicidial_calls IS NULL) THEN 1
        WHEN c.tipo_phone IN ('movil_2', 'movil_3') THEN 2
        WHEN c.tipo_phone = 'movil_4' AND c.vicidial_calls > 3 THEN 3
        ELSE 4
    END ASC,
    c.vicidial_calls ASC;
