-- =====================================================
-- STORED PROCEDURE: CONSOLIDAR DATOS PARA MODELO
-- Versión: 5.0 (Filtros corregidos)
-- Autor: Juan David Ramírez
-- Fecha: Diciembre 2025
-- CAMBIOS: 
-- - Fecha desde septiembre (cuando hay datos completos)
-- - Excluye bases especiales
-- =====================================================

USE bbdd_cos_bog_colsubsidio_agencia_empleo;

DROP PROCEDURE IF EXISTS sp_consolidar_datos_modelo_conversion;

DELIMITER $$

CREATE PROCEDURE sp_consolidar_datos_modelo_conversion()
BEGIN
    
    -- Limpiar tabla antes de insertar
    TRUNCATE TABLE tb_modelo_conversion_intermediacion;
    
    -- Insertar datos consolidados con categorizaciones
    INSERT INTO tb_modelo_conversion_intermediacion (
        no_documento,
        codigo_unico_vacante,
        tipo_de_gestion,
        numero_de_vacantes_original,
        cargo_original,
        empresa_original,
        requisito_profesional_original,
        persona_contacto_empresa,
        hora_entrevista_original,
        fecha_asignacion,
        documentacion_requerida_original,
        rango_vacantes,
        categoria_cargo,
        categoria_empresa,
        categoria_requisito,
        tiene_contacto_empresa,
        franja_hora_entrevista,
        categoria_documentacion,
        Tipo_contacto,
        Homologacion_tipificacion,
        Contacto,
        Gestionado,
        Sin_gestion,
        No_contacto,
        Efectividad,
        intentos,
        duracion_llamada,
        Fuente,
        conversion
    )
    SELECT 
        a.no_documento,
        CONCAT(a.no_documento, a.codigo_vacante) AS codigo_unico_vacante,
        a.tipo_de_gestion,
        a.numero_de_vacantes AS numero_de_vacantes_original,
        a.cargo AS cargo_original,
        a.empresa AS empresa_original,
        a.requisito_profesional AS requisito_profesional_original,
        a.persona_contacto_empresa,
        a.hora_entrevista AS hora_entrevista_original,
        a.fecha_asignacion,
        a.documentacion_requerida AS documentacion_requerida_original,
        
        -- CATEGORIZACIÓN: RANGO DE VACANTES
        CASE
            WHEN a.numero_de_vacantes < 100 THEN '1-100'
            WHEN a.numero_de_vacantes < 200 THEN '100-200'
            WHEN a.numero_de_vacantes < 300 THEN '200-300'
            WHEN a.numero_de_vacantes >= 300 THEN '300+'
            ELSE 'sin_informacion'
        END AS rango_vacantes,
        
        -- CATEGORIZACIÓN: CARGO
        CASE
            WHEN UPPER(a.cargo) REGEXP 'LIMPIEZA|ASEO|SERVICIOS GENERALES|DESINFECCION' THEN 'limpieza_aseo'
            WHEN UPPER(a.cargo) REGEXP 'SEGURIDAD|VIGILANTE|GUARDA' THEN 'seguridad'
            WHEN UPPER(a.cargo) REGEXP 'BODEGA|LOGISTIC|ALMACEN' THEN 'bodega_logistica'
            WHEN UPPER(a.cargo) REGEXP 'PRODUCCION|OPERARIO|EMPAQUE|MANUFACTUR' THEN 'produccion'
            WHEN UPPER(a.cargo) REGEXP 'COMERCIAL|VENTAS|ASESOR.*COMERCIAL|EJECUTIVO.*VENTAS|IMPULSA' THEN 'ventas_comercial'
            WHEN UPPER(a.cargo) REGEXP 'CALL CENTER|CONTACT CENTER|TELEMARKETING' THEN 'call_center'
            WHEN UPPER(a.cargo) REGEXP 'COCINA|COCINERO|MESERO|PANADERO' THEN 'cocina_alimentos'
            WHEN UPPER(a.cargo) REGEXP 'CONDUCTOR|MOTORIZADO|DOMICILIARIO|TRANSPORTA' THEN 'conductor_transporte'
            WHEN UPPER(a.cargo) REGEXP 'TECNICO|ELECTRICISTA|MECANICO|MANTENIMIENTO' THEN 'tecnico'
            WHEN UPPER(a.cargo) REGEXP 'MEDICO|ODONTOLOGO|ENFERMERA|TERAPEUTA|OPTOMETRA' THEN 'profesional_salud'
            WHEN UPPER(a.cargo) REGEXP 'AUXILIAR' AND NOT UPPER(a.cargo) REGEXP 'BODEGA|LOGISTIC|PRODUCCION|LIMPIEZA' THEN 'auxiliar_general'
            WHEN UPPER(a.cargo) REGEXP 'CAJERO' THEN 'cajero'
            WHEN UPPER(a.cargo) REGEXP 'ARCHIVO|DIGITACION' THEN 'archivo_digitacion'
            ELSE 'otros'
        END AS categoria_cargo,
        
        -- CATEGORIZACIÓN: EMPRESA
        CASE
            WHEN UPPER(a.empresa) REGEXP 'CASALIMPIA|ASEOS|PROLIMZA|ECOLIMPIEZA|LOGISTICA Y LIMPIEZA|NASE|EXPERIENZA|ASER ASEO|CASA LIMPIA|ADMIASEO' THEN 'limpieza'
            WHEN UPPER(a.empresa) REGEXP 'SECURITAS|VISE|SEGURIDAD NACIONAL|LIBERTADORA|PROSEGUR|COLVISEG|HONOR|TRANSBANK' THEN 'seguridad'
            WHEN UPPER(a.empresa) REGEXP 'MANPOWER|ADECCO|PRODUCTIVIDAD EMPRESARIAL|TEMPORAL|SOLUCIONES LABORALES|GOLD RH|FLEXITEMP|GESTION TEMPORAL|ACTIVOS|MULTIEMPLEOS|COMPLEMENTOS HUMANOS|ELITE|GRUPO SOLUCIONES|INTEGRITY|SOLUCIONES INMEDIATAS|QUICK HELP|JOB AND TALENT|JOBANDTALENT' THEN 'servicios_temporales'
            WHEN UPPER(a.empresa) REGEXP 'D1|ARA|JERONIMO MARTINS|ALKOSTO|EXITO|GRUPO EXITO|MINISO' THEN 'retail'
            WHEN UPPER(a.empresa) REGEXP 'ATENTO|DICO|TELEPERFORMANCE|ATECH BPO|IMAGE QUALITY|BRM|MANEJO TECNICO' THEN 'bpo_callcenter'
            WHEN UPPER(a.empresa) REGEXP 'HERMECO|OFFCORSS|AJOVER|DARNEL|CASA LUKER|PERMODA|KOAJ|MODANOVA|MANUFACTURAS ELIOT' THEN 'manufactura_textil'
            WHEN UPPER(a.empresa) REGEXP 'LADRILLERA|PROTELA|FORTOX|COLOMBIANA DE PINTURAS|FERROALUMINIOS|FLEXO SPRING|ESTIBAS' THEN 'manufactura_industrial'
            WHEN UPPER(a.empresa) REGEXP 'FRISBY|HORNITOS|LISTOS|QUALA|GOYURT|TABASCO|GATE GOURMET|GATEGOURMET' THEN 'alimentos'
            WHEN UPPER(a.empresa) REGEXP 'CONSORCIO EXPRESS|GMOVIL|DITRANSA|COTRANSCOPETROL|ANAVA TRANSPORT|DISTRIBUCIONES AXA|MEGALINEA' THEN 'transporte_logistica'
            WHEN UPPER(a.empresa) REGEXP 'EMERMEDICA|KERALTY|AGM SALUD|GLOBAL LIFE' THEN 'salud'
            WHEN UPPER(a.empresa) REGEXP 'RECUPERAR|RECAUDO|CONSULTORA.*CARTERA' THEN 'cobranza'
            WHEN UPPER(a.empresa) REGEXP 'CONSTRUCTORA|CONSTRUELECTRICOS|CONSTRUCCION Y MANTENIMIENTO' THEN 'construccion'
            WHEN UPPER(a.empresa) REGEXP 'THOMAS GREG|TAESCOL|OPTICENTRO|SERDAN|RECORDAR|PREVISION EXEQUIAL|FUMIGACION|QUALITY CARWASH|SIMONIZ' THEN 'servicios_especializados'
            WHEN UPPER(a.empresa) REGEXP 'INVERSIONES EL CARNAL|VENTAS Y SERVICIOS|CALZATODO|KARROMANIA' THEN 'ventas_comercial'
            WHEN UPPER(a.empresa) REGEXP 'COLSUBSIDIO' THEN 'colsubsidio'
            WHEN UPPER(a.empresa) REGEXP 'AGROPECUARIA|FLORICULTOR|AVICOLA' THEN 'agropecuaria'
            ELSE 'otras'
        END AS categoria_empresa,
        
        -- CATEGORIZACIÓN: REQUISITO PROFESIONAL
        CASE
            WHEN a.requisito_profesional = '-' OR a.requisito_profesional IS NULL OR TRIM(a.requisito_profesional) = '' THEN 'sin_especificar'
            WHEN UPPER(a.requisito_profesional) REGEXP 'PROFESIONAL|LICENCIATURA|MEDICO|ODONTOLOGO|ARQUITECTO|INGENIERO' THEN 'profesional'
            WHEN UPPER(a.requisito_profesional) REGEXP 'TECNOLOGO' THEN 'tecnologo'
            WHEN UPPER(a.requisito_profesional) REGEXP 'TECNICO|CURSO.*VIGILANCIA|RETHUS' THEN 'tecnico'
            WHEN UPPER(a.requisito_profesional) REGEXP 'BACHILLER' THEN 'bachiller'
            WHEN UPPER(a.requisito_profesional) REGEXP 'PRIMARIA|LECTO|NOVENO|9' THEN 'basica'
            WHEN UPPER(a.requisito_profesional) REGEXP 'NO APLICA|SIN EXPERIENCIA' THEN 'sin_requisito'
            ELSE 'otros'
        END AS categoria_requisito,
        
        -- TIENE CONTACTO EMPRESA
        CASE 
            WHEN a.persona_contacto_empresa IS NOT NULL 
                 AND TRIM(a.persona_contacto_empresa) != '' 
                 AND a.persona_contacto_empresa != '-' 
            THEN 1 
            ELSE 0 
        END AS tiene_contacto_empresa,
        
        -- CATEGORIZACIÓN: FRANJA HORARIA
        CASE
            WHEN a.hora_entrevista IS NULL OR TRIM(a.hora_entrevista) = '' OR a.hora_entrevista = '-' THEN 'sin_hora'
            WHEN a.hora_entrevista REGEXP '-' THEN 'multiple'
            WHEN UPPER(a.hora_entrevista) REGEXP '07:|08:|09:|10:|11:' OR UPPER(a.hora_entrevista) REGEXP 'AM' THEN 'manana'
            WHEN UPPER(a.hora_entrevista) REGEXP '12:|01:|02:|03:|04:|05:' OR UPPER(a.hora_entrevista) REGEXP 'PM' THEN 'tarde'
            ELSE 'sin_hora'
        END AS franja_hora_entrevista,
        
        -- CATEGORIZACIÓN: DOCUMENTACIÓN
        CASE
            WHEN a.documentacion_requerida = '-' OR a.documentacion_requerida IS NULL OR TRIM(a.documentacion_requerida) = '' THEN 'sin_especificar'
            WHEN UPPER(a.documentacion_requerida) REGEXP 'ANTECEDENTES|ADRES|LIBRETA|CERTIFICADO.*PENSION|CURSO' THEN 'completa'
            WHEN UPPER(a.documentacion_requerida) REGEXP 'HOJA.*VIDA.*CEDULA.*CERTIFICADO|SOPORTE' THEN 'media'
            WHEN UPPER(a.documentacion_requerida) REGEXP 'HOJA.*VIDA|CEDULA|DOCUMENTO' THEN 'basica'
            ELSE 'otros'
        END AS categoria_documentacion,
        
        -- FEATURES DE INFORME
        i.Tipo_contacto,
        i.Homologacion_tipificacion,
        i.Contacto,
        i.Gestionado,
        i.Sin_gestion,
        i.No_contacto,
        i.Efectividad,
        i.intentos,
        i.duracion_llamada,
        i.Fuente,
        
        -- VARIABLE OBJETIVO
        i.Conversion
        
    FROM 
        bbdd_cos_bog_colsubsidio_agencia_empleo.tb_asignacion_intermediacion_v2 a
    INNER JOIN 
        bbdd_cos_bog_colsubsidio_agencia_empleo.tb_informe_intermediacion i
        ON CONCAT(a.no_documento, a.codigo_vacante) = i.Codigo_unico_vacante
    WHERE 
        a.fecha_asignacion >= '2025-09-01'  -- ✅ DESDE SEPTIEMBRE (datos completos)
        AND i.nombre_base <> 'BASES ESPECIALES'  -- ✅ EXCLUIR BASES ESPECIALES
        AND i.Conversion IS NOT NULL  
        AND i.razon_no_apto IS NULL;
    
    -- Mostrar resumen
    SELECT 
        COUNT(*) AS total_registros_insertados,
        SUM(conversion) AS total_conversiones,
        ROUND(AVG(conversion) * 100, 2) AS tasa_conversion_porcentaje
    FROM tb_modelo_conversion_intermediacion;
    
END$$

DELIMITER ;

-- =====================================================
-- EJECUCIÓN
-- =====================================================
-- CALL sp_consolidar_datos_modelo_conversion();
