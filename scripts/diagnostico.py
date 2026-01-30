"""
Script de diagnostico - Modelo de Conversión
Analiza las predicciones sin generar archivos Excel
"""

import sys
import os
from datetime import datetime
import pandas as pd
import numpy as np
import pickle
import warnings
warnings.filterwarnings('ignore')

# Agregar path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from database import get_engine, query_to_dataframe

def cargar_modelo():
    """Cargar el modelo entrenado y las columnas"""
    try:
        model_path = os.path.join(os.path.dirname(__file__), '..', 'models', 'modelo_conversion_precontacto.pkl')
        columns_path = os.path.join(os.path.dirname(__file__), '..', 'models', 'model_columns_precontacto.pkl')
        
        with open(model_path, 'rb') as f:
            modelo = pickle.load(f)
        
        with open(columns_path, 'rb') as f:
            columnas_modelo = pickle.load(f)
        
        print("✅ Modelo cargado")
        return modelo, columnas_modelo
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, None

def cargar_query_sql(nombre_archivo):
    """Cargar query SQL desde archivo"""
    try:
        sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', nombre_archivo)
        with open(sql_path, 'r', encoding='utf-8') as f:
            query = f.read()
        return query
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def obtener_asignaciones_nuevas(engine):
    """Obtener asignaciones del día"""
    query = cargar_query_sql('query_asignaciones_diarias.sql')
    if query is None:
        return None
    
    try:
        df = query_to_dataframe(query, engine)
        print(f"✅ Asignaciones: {len(df):,}")
        return df
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def preprocesar_datos(df):
    """Aplicar preprocesamiento"""
    
    df_procesado = df.copy()
    
    # RANGO DE VACANTES
    df_procesado['numero_de_vacantes'] = pd.to_numeric(df_procesado['numero_de_vacantes'], errors='coerce')
    df_procesado['rango_vacantes'] = df_procesado['numero_de_vacantes'].apply(lambda x: 
        '1-100' if x < 100 else
        '100-200' if x < 200 else
        '200-300' if x < 300 else
        '300+' if x >= 300 else
        'sin_informacion'
    )
    
    # CATEGORIA CARGO
    def categorizar_cargo(cargo):
        if pd.isna(cargo):
            return 'otros'
        cargo_upper = str(cargo).upper()
        if any(word in cargo_upper for word in ['LIMPIEZA', 'ASEO', 'SERVICIOS GENERALES', 'DESINFECCION']):
            return 'limpieza_aseo'
        elif any(word in cargo_upper for word in ['SEGURIDAD', 'VIGILANTE', 'GUARDA']):
            return 'seguridad'
        elif any(word in cargo_upper for word in ['BODEGA', 'LOGISTIC', 'ALMACEN']):
            return 'bodega_logistica'
        elif any(word in cargo_upper for word in ['PRODUCCION', 'OPERARIO', 'EMPAQUE', 'MANUFACTUR']):
            return 'produccion'
        elif any(word in cargo_upper for word in ['COMERCIAL', 'VENTAS', 'ASESOR', 'EJECUTIVO', 'IMPULSA']):
            return 'ventas_comercial'
        elif any(word in cargo_upper for word in ['CALL CENTER', 'CONTACT CENTER', 'TELEMARKETING']):
            return 'call_center'
        elif any(word in cargo_upper for word in ['COCINA', 'COCINERO', 'MESERO', 'PANADERO']):
            return 'cocina_alimentos'
        elif any(word in cargo_upper for word in ['CONDUCTOR', 'MOTORIZADO', 'DOMICILIARIO', 'TRANSPORTA']):
            return 'conductor_transporte'
        elif any(word in cargo_upper for word in ['TECNICO', 'ELECTRICISTA', 'MECANICO', 'MANTENIMIENTO']):
            return 'tecnico'
        elif any(word in cargo_upper for word in ['MEDICO', 'ODONTOLOGO', 'ENFERMERA', 'TERAPEUTA', 'OPTOMETRA']):
            return 'profesional_salud'
        elif 'AUXILIAR' in cargo_upper and not any(word in cargo_upper for word in ['BODEGA', 'LOGISTIC', 'PRODUCCION', 'LIMPIEZA']):
            return 'auxiliar_general'
        elif 'CAJERO' in cargo_upper:
            return 'cajero'
        elif any(word in cargo_upper for word in ['ARCHIVO', 'DIGITACION']):
            return 'archivo_digitacion'
        else:
            return 'otros'
    
    df_procesado['categoria_cargo'] = df_procesado['cargo'].apply(categorizar_cargo)
    
    # CATEGORIA EMPRESA
    def categorizar_empresa(empresa):
        if pd.isna(empresa):
            return 'otras'
        empresa_upper = str(empresa).upper()
        if any(word in empresa_upper for word in ['CASALIMPIA', 'ASEOS', 'PROLIMZA', 'ECOLIMPIEZA', 'NASE', 'EXPERIENZA', 'CASA LIMPIA', 'ADMIASEO']):
            return 'limpieza'
        elif any(word in empresa_upper for word in ['SECURITAS', 'VISE', 'SEGURIDAD NACIONAL', 'LIBERTADORA', 'PROSEGUR', 'COLVISEG', 'HONOR', 'TRANSBANK']):
            return 'seguridad'
        elif any(word in empresa_upper for word in ['MANPOWER', 'ADECCO', 'PRODUCTIVIDAD EMPRESARIAL', 'TEMPORAL', 'SOLUCIONES LABORALES', 'GOLD RH', 'FLEXITEMP', 'ACTIVOS', 'MULTIEMPLEOS', 'COMPLEMENTOS HUMANOS', 'ELITE', 'INTEGRITY', 'JOB AND TALENT', 'JOBANDTALENT']):
            return 'servicios_temporales'
        elif any(word in empresa_upper for word in ['D1', 'ARA', 'JERONIMO MARTINS', 'ALKOSTO', 'EXITO', 'MINISO']):
            return 'retail'
        elif any(word in empresa_upper for word in ['ATENTO', 'DICO', 'TELEPERFORMANCE', 'ATECH BPO', 'IMAGE QUALITY', 'BRM', 'MANEJO TECNICO']):
            return 'bpo_callcenter'
        elif any(word in empresa_upper for word in ['HERMECO', 'OFFCORSS', 'AJOVER', 'DARNEL', 'CASA LUKER', 'PERMODA', 'KOAJ', 'MODANOVA', 'MANUFACTURAS ELIOT']):
            return 'manufactura_textil'
        elif any(word in empresa_upper for word in ['LADRILLERA', 'PROTELA', 'FORTOX', 'COLOMBIANA DE PINTURAS', 'FERROALUMINIOS', 'FLEXO SPRING', 'ESTIBAS']):
            return 'manufactura_industrial'
        elif any(word in empresa_upper for word in ['FRISBY', 'HORNITOS', 'LISTOS', 'QUALA', 'GOYURT', 'TABASCO', 'GATE GOURMET', 'GATEGOURMET']):
            return 'alimentos'
        elif any(word in empresa_upper for word in ['CONSORCIO EXPRESS', 'GMOVIL', 'DITRANSA', 'COTRANSCOPETROL', 'ANAVA TRANSPORT', 'DISTRIBUCIONES AXA', 'MEGALINEA']):
            return 'transporte_logistica'
        elif any(word in empresa_upper for word in ['EMERMEDICA', 'KERALTY', 'AGM SALUD', 'GLOBAL LIFE']):
            return 'salud'
        elif any(word in empresa_upper for word in ['RECUPERAR', 'RECAUDO', 'CONSULTORA', 'CARTERA']):
            return 'cobranza'
        elif any(word in empresa_upper for word in ['CONSTRUCTORA', 'CONSTRUELECTRICOS', 'CONSTRUCCION']):
            return 'construccion'
        elif any(word in empresa_upper for word in ['THOMAS GREG', 'TAESCOL', 'OPTICENTRO', 'SERDAN', 'RECORDAR', 'PREVISION EXEQUIAL', 'FUMIGACION', 'QUALITY CARWASH', 'SIMONIZ']):
            return 'servicios_especializados'
        elif any(word in empresa_upper for word in ['INVERSIONES EL CARNAL', 'VENTAS Y SERVICIOS', 'CALZATODO', 'KARROMANIA']):
            return 'ventas_comercial'
        elif 'COLSUBSIDIO' in empresa_upper:
            return 'colsubsidio'
        elif any(word in empresa_upper for word in ['AGROPECUARIA', 'FLORICULTOR', 'AVICOLA']):
            return 'agropecuaria'
        else:
            return 'otras'
    
    df_procesado['categoria_empresa'] = df_procesado['empresa'].apply(categorizar_empresa)
    
    # CATEGORIA REQUISITO
    def categorizar_requisito(requisito):
        if pd.isna(requisito) or str(requisito).strip() in ['', '-']:
            return 'sin_especificar'
        req_upper = str(requisito).upper()
        if any(word in req_upper for word in ['PROFESIONAL', 'LICENCIATURA', 'MEDICO', 'ODONTOLOGO', 'ARQUITECTO', 'INGENIERO']):
            return 'profesional'
        elif 'TECNOLOGO' in req_upper:
            return 'tecnologo'
        elif any(word in req_upper for word in ['TECNICO', 'CURSO', 'VIGILANCIA', 'RETHUS']):
            return 'tecnico'
        elif 'BACHILLER' in req_upper:
            return 'bachiller'
        elif any(word in req_upper for word in ['PRIMARIA', 'LECTO', 'NOVENO', '9']):
            return 'basica'
        elif any(word in req_upper for word in ['NO APLICA', 'SIN EXPERIENCIA']):
            return 'sin_requisito'
        else:
            return 'otros'
    
    df_procesado['categoria_requisito'] = df_procesado['requisito_profesional'].apply(categorizar_requisito)
    df_procesado['tiene_contacto_empresa'] = df_procesado['persona_contacto_empresa'].notna().astype(int)
    
    # FRANJA HORA
    def categorizar_hora(hora):
        if pd.isna(hora) or str(hora).strip() in ['', '-']:
            return 'sin_hora'
        hora_str = str(hora).upper()
        if '-' in hora_str:
            return 'multiple'
        elif any(h in hora_str for h in ['07:', '08:', '09:', '10:', '11:']) or 'AM' in hora_str:
            return 'manana'
        elif any(h in hora_str for h in ['12:', '01:', '02:', '03:', '04:', '05:']) or 'PM' in hora_str:
            return 'tarde'
        else:
            return 'sin_hora'
    
    df_procesado['franja_hora_entrevista'] = df_procesado['hora_entrevista'].apply(categorizar_hora)
    
    # DOCUMENTACION
    def categorizar_documentacion(doc):
        if pd.isna(doc) or str(doc).strip() in ['', '-']:
            return 'sin_especificar'
        doc_upper = str(doc).upper()
        if any(word in doc_upper for word in ['ANTECEDENTES', 'ADRES', 'LIBRETA', 'PENSION', 'CURSO']):
            return 'completa'
        elif any(word in doc_upper for word in ['HOJA', 'VIDA', 'CEDULA', 'CERTIFICADO', 'SOPORTE']):
            return 'media'
        elif any(word in doc_upper for word in ['HOJA', 'VIDA', 'CEDULA', 'DOCUMENTO']):
            return 'basica'
        else:
            return 'otros'
    
    df_procesado['categoria_documentacion'] = df_procesado['documentacion_requerida'].apply(categorizar_documentacion)
    
    # TEMPORALES
    df_procesado['fecha_asignacion'] = pd.to_datetime(df_procesado['fecha_asignacion'])
    df_procesado['dia_semana'] = df_procesado['fecha_asignacion'].dt.dayofweek
    df_procesado['mes'] = df_procesado['fecha_asignacion'].dt.month
    df_procesado['es_fin_semana'] = (df_procesado['dia_semana'] >= 5).astype(int)
    
    return df_procesado

def main():
    """Diagnóstico"""
    
    print("\n" + "="*50)
    print("🔍 DIAGNÓSTICO DEL MODELO")
    print("="*50 + "\n")
    
    modelo, columnas_modelo = cargar_modelo()
    if modelo is None:
        return
    
    engine = get_engine()
    if engine is None:
        return
    
    df_asignaciones = obtener_asignaciones_nuevas(engine)
    if df_asignaciones is None or len(df_asignaciones) == 0:
        print("⚠️ Sin asignaciones")
        return
    
    print("\n" + "="*50)
    print("📋 DATOS ORIGINALES")
    print("="*50)
    print(df_asignaciones[['empresa', 'cargo', 'tipo_de_gestion', 'numero_de_vacantes']].head())
    
    df_procesado = preprocesar_datos(df_asignaciones)
    
    print("\n" + "="*50)
    print("🏷️ DATOS CATEGORIZADOS")
    print("="*50)
    print(df_procesado[['empresa', 'categoria_empresa', 'cargo', 'categoria_cargo', 'rango_vacantes']].head())
    
    print("\n" + "="*50)
    print("📊 DISTRIBUCIÓN DE CATEGORÍAS")
    print("="*50)
    print("\n🏢 Top 10 Empresas:")
    print(df_procesado['categoria_empresa'].value_counts().head(10))
    print("\n👔 Top 10 Cargos:")
    print(df_procesado['categoria_cargo'].value_counts().head(10))
    
    # Preparar para modelo
    categorical_features = [
        'tipo_de_gestion',
        'rango_vacantes',
        'categoria_cargo',
        'categoria_empresa',
        'categoria_requisito',
        'franja_hora_entrevista',
        'categoria_documentacion'
    ]
    
    numeric_features = [
        'tiene_contacto_empresa',
        'dia_semana',
        'mes',
        'es_fin_semana'
    ]
    
    df_encoded = pd.get_dummies(
        df_procesado[categorical_features + numeric_features],
        columns=categorical_features,
        drop_first=True
    )
    
    for col in columnas_modelo:
        if col not in df_encoded.columns:
            df_encoded[col] = 0
    
    df_encoded = df_encoded[columnas_modelo]
    
    probabilidades = modelo.predict_proba(df_encoded)[:, 1]
    
    print("\n" + "="*50)
    print("📊 PROBABILIDADES")
    print("="*50)
    print(f"Mínima:   {probabilidades.min():.4f}")
    print(f"Máxima:   {probabilidades.max():.4f}")
    print(f"Promedio: {probabilidades.mean():.4f}")
    print(f"Mediana:  {np.median(probabilidades):.4f}")
    
    print("\n📊 Distribución:")
    print(f"  < 20%:  {sum(probabilidades < 0.20):,} ({sum(probabilidades < 0.20)/len(probabilidades)*100:.1f}%)")
    print(f"  20-30%: {sum((probabilidades >= 0.20) & (probabilidades < 0.30)):,} ({sum((probabilidades >= 0.20) & (probabilidades < 0.30))/len(probabilidades)*100:.1f}%)")
    print(f"  30-40%: {sum((probabilidades >= 0.30) & (probabilidades < 0.40)):,} ({sum((probabilidades >= 0.30) & (probabilidades < 0.40))/len(probabilidades)*100:.1f}%)")
    print(f"  40-50%: {sum((probabilidades >= 0.40) & (probabilidades < 0.50)):,} ({sum((probabilidades >= 0.40) & (probabilidades < 0.50))/len(probabilidades)*100:.1f}%)")
    print(f"  > 50%:  {sum(probabilidades >= 0.50):,} ({sum(probabilidades >= 0.50)/len(probabilidades)*100:.1f}%)")
    
    df_resultado = df_procesado.copy()
    df_resultado['probabilidad'] = probabilidades
    
    print("\n" + "="*50)
    print("🔝 TOP 10 - Mayores probabilidades")
    print("="*50)
    print(df_resultado.nlargest(10, 'probabilidad')[['empresa', 'cargo', 'probabilidad']])
    
    print("\n" + "="*50)
    print("🔻 BOTTOM 10 - Menores probabilidades")
    print("="*50)
    print(df_resultado.nsmallest(10, 'probabilidad')[['empresa', 'cargo', 'probabilidad']])
    
    print("\n" + "="*50)
    print("⚖️ COMPARACIÓN")
    print("="*50)
    print("Training: promedio ~47%")
    print(f"Hoy: promedio {probabilidades.mean()*100:.1f}%")
    print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    main()
