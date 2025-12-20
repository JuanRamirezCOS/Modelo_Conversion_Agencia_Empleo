WITH registros_numerados AS (
    SELECT 
        i.*,
        ROW_NUMBER() OVER (
            PARTITION BY i.no_documento, i.codigo_vacante 
            ORDER BY i.fecha_asignacion ASC
        ) AS orden
    FROM bbdd_cos_bog_colsubsidio_agencia_empleo.tb_asignacion_intermediacion_v2 i
    WHERE i.fecha_asignacion >= CURDATE()
),
duplicados AS (
    SELECT no_documento, codigo_vacante
    FROM bbdd_cos_bog_colsubsidio_agencia_empleo.tb_asignacion_intermediacion_v2_duplicados
    WHERE creation_time >= CURDATE()
)
SELECT 
    tipo_de_gestion,
    numero_de_vacantes,
    codigo_vacante,
    cargo,
    empresa,
    requisito_profesional,
    persona_contacto_empresa,
    hora_entrevista,
    fecha_asignacion,
    documentacion_requerida,
    fecha_entrevista,
    no_documento,
    nombres,
    apellidos,
    movil_1 AS phone,
    email,
    CONCAT(no_documento, codigo_vacante) AS codigo_unico_vacante
FROM registros_numerados r
WHERE 
    -- Solo registros válidos (documento y móvil correctos) o el primer duplicado válido
    (no_documento IS NOT NULL AND no_documento != '' AND no_documento REGEXP '^[0-9]{6,11}$')
    AND (movil_1 IS NOT NULL AND movil_1 != '' AND movil_1 REGEXP '^(3[0-9]{9}|60[0-9]{8})$')
    AND (
        -- No está duplicado
        NOT EXISTS (SELECT 1 FROM duplicados d WHERE d.no_documento = r.no_documento AND d.codigo_vacante = r.codigo_vacante)
        OR 
        -- O es el primer duplicado (orden = 1)
        (EXISTS (SELECT 1 FROM duplicados d WHERE d.no_documento = r.no_documento AND d.codigo_vacante = r.codigo_vacante) AND r.orden = 1)
    )
ORDER BY fecha_asignacion DESC;
