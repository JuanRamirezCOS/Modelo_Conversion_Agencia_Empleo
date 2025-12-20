"""
Script de Predicción Diaria - Modelo de Conversión
Genera scores de priorización para nuevas asignaciones
"""

import sys
import os
from datetime import datetime
import pandas as pd
import pickle
import warnings
warnings.filterwarnings('ignore')

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from database import get_engine, query_to_dataframe

def cargar_modelo():
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
    try:
        sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', nombre_archivo)
        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def obtener_asignaciones_nuevas(engine):
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
    df_procesado = df.copy()
    
    # RANGO VACANTES
    df_procesado['numero_de_vacantes'] = pd.to_numeric(df_procesado['numero_de_vacantes'], errors='coerce')
    df_procesado['rango_vacantes'] = df_procesado['numero_de_vacantes'].apply(lambda x: 
        '1-100' if x < 100 else '100-200' if x < 200 else '200-300' if x < 300 else '300+' if x >= 300 else 'sin_informacion')
    
    # CATEGORIAS (funciones simplificadas - mismo código que antes)
    def categorizar_cargo(cargo):
        if pd.isna(cargo): return 'otros'
        cargo_upper = str(cargo).upper()
        if any(w in cargo_upper for w in ['LIMPIEZA', 'ASEO', 'SERVICIOS GENERALES']): return 'limpieza_aseo'
        elif any(w in cargo_upper for w in ['SEGURIDAD', 'VIGILANTE']): return 'seguridad'
        elif any(w in cargo_upper for w in ['BODEGA', 'LOGISTIC', 'ALMACEN']): return 'bodega_logistica'
        elif any(w in cargo_upper for w in ['PRODUCCION', 'OPERARIO', 'EMPAQUE']): return 'produccion'
        elif any(w in cargo_upper for w in ['COMERCIAL', 'VENTAS', 'ASESOR']): return 'ventas_comercial'
        elif 'CALL CENTER' in cargo_upper or 'CONTACT CENTER' in cargo_upper: return 'call_center'
        elif any(w in cargo_upper for w in ['COCINA', 'COCINERO', 'MESERO']): return 'cocina_alimentos'
        elif any(w in cargo_upper for w in ['CONDUCTOR', 'MOTORIZADO', 'DOMICILIARIO']): return 'conductor_transporte'
        elif any(w in cargo_upper for w in ['TECNICO', 'ELECTRICISTA', 'MECANICO']): return 'tecnico'
        elif any(w in cargo_upper for w in ['MEDICO', 'ODONTOLOGO', 'ENFERMERA']): return 'profesional_salud'
        elif 'AUXILIAR' in cargo_upper and not any(w in cargo_upper for w in ['BODEGA', 'LOGISTIC', 'PRODUCCION']): return 'auxiliar_general'
        elif 'CAJERO' in cargo_upper: return 'cajero'
        elif any(w in cargo_upper for w in ['ARCHIVO', 'DIGITACION']): return 'archivo_digitacion'
        else: return 'otros'
    
    def categorizar_empresa(empresa):
        if pd.isna(empresa): return 'otras'
        e = str(empresa).upper()
        if any(w in e for w in ['CASALIMPIA', 'ASEOS', 'ECOLIMPIEZA', 'NASE']): return 'limpieza'
        elif any(w in e for w in ['SECURITAS', 'VISE', 'SEGURIDAD NACIONAL', 'LIBERTADORA']): return 'seguridad'
        elif any(w in e for w in ['MANPOWER', 'ADECCO', 'PRODUCTIVIDAD EMPRESARIAL', 'TEMPORAL', 'FLEXITEMP']): return 'servicios_temporales'
        elif any(w in e for w in ['D1', 'ARA', 'ALKOSTO', 'EXITO']): return 'retail'
        elif any(w in e for w in ['ATENTO', 'DICO', 'TELEPERFORMANCE', 'BRM', 'MANEJO TECNICO']): return 'bpo_callcenter'
        elif any(w in e for w in ['HERMECO', 'OFFCORSS', 'PERMODA', 'KOAJ']): return 'manufactura_textil'
        elif any(w in e for w in ['LADRILLERA', 'PROTELA', 'FORTOX']): return 'manufactura_industrial'
        elif any(w in e for w in ['FRISBY', 'LISTOS', 'QUALA', 'GOYURT']): return 'alimentos'
        elif any(w in e for w in ['CONSORCIO EXPRESS', 'GMOVIL', 'DITRANSA']): return 'transporte_logistica'
        elif any(w in e for w in ['EMERMEDICA', 'KERALTY', 'AGM SALUD']): return 'salud'
        elif any(w in e for w in ['RECUPERAR', 'RECAUDO', 'CARTERA']): return 'cobranza'
        elif any(w in e for w in ['CONSTRUCTORA', 'CONSTRUCCION']): return 'construccion'
        elif any(w in e for w in ['THOMAS GREG', 'OPTICENTRO', 'SERDAN']): return 'servicios_especializados'
        elif any(w in e for w in ['INVERSIONES EL CARNAL', 'CALZATODO']): return 'ventas_comercial'
        elif 'COLSUBSIDIO' in e: return 'colsubsidio'
        elif any(w in e for w in ['AGROPECUARIA', 'FLORICULTOR']): return 'agropecuaria'
        else: return 'otras'
    
    def categorizar_requisito(req):
        if pd.isna(req) or str(req).strip() in ['', '-']: return 'sin_especificar'
        r = str(req).upper()
        if any(w in r for w in ['PROFESIONAL', 'MEDICO', 'INGENIERO']): return 'profesional'
        elif 'TECNOLOGO' in r: return 'tecnologo'
        elif any(w in r for w in ['TECNICO', 'CURSO', 'VIGILANCIA']): return 'tecnico'
        elif 'BACHILLER' in r: return 'bachiller'
        elif any(w in r for w in ['PRIMARIA', 'NOVENO']): return 'basica'
        elif 'NO APLICA' in r: return 'sin_requisito'
        else: return 'otros'
    
    def categorizar_hora(hora):
        if pd.isna(hora) or str(hora).strip() in ['', '-']: return 'sin_hora'
        h = str(hora).upper()
        if '-' in h: return 'multiple'
        elif any(x in h for x in ['07:', '08:', '09:', '10:', '11:', 'AM']): return 'manana'
        elif any(x in h for x in ['12:', '01:', '02:', '03:', '04:', '05:', 'PM']): return 'tarde'
        else: return 'sin_hora'
    
    def categorizar_doc(doc):
        if pd.isna(doc) or str(doc).strip() in ['', '-']: return 'sin_especificar'
        d = str(doc).upper()
        if any(w in d for w in ['ANTECEDENTES', 'ADRES', 'LIBRETA', 'PENSION']): return 'completa'
        elif any(w in d for w in ['HOJA', 'VIDA', 'CEDULA', 'CERTIFICADO']): return 'media'
        elif 'DOCUMENTO' in d: return 'basica'
        else: return 'otros'
    
    df_procesado['categoria_cargo'] = df_procesado['cargo'].apply(categorizar_cargo)
    df_procesado['categoria_empresa'] = df_procesado['empresa'].apply(categorizar_empresa)
    df_procesado['categoria_requisito'] = df_procesado['requisito_profesional'].apply(categorizar_requisito)
    df_procesado['tiene_contacto_empresa'] = df_procesado['persona_contacto_empresa'].notna().astype(int)
    df_procesado['franja_hora_entrevista'] = df_procesado['hora_entrevista'].apply(categorizar_hora)
    df_procesado['categoria_documentacion'] = df_procesado['documentacion_requerida'].apply(categorizar_doc)
    
    df_procesado['fecha_asignacion'] = pd.to_datetime(df_procesado['fecha_asignacion'])
    df_procesado['dia_semana'] = df_procesado['fecha_asignacion'].dt.dayofweek
    df_procesado['mes'] = df_procesado['fecha_asignacion'].dt.month
    df_procesado['es_fin_semana'] = (df_procesado['dia_semana'] >= 5).astype(int)
    
    print("✅ Datos preprocesados")
    return df_procesado

def generar_predicciones(df_procesado, modelo, columnas_modelo):
    categorical_features = ['tipo_de_gestion', 'rango_vacantes', 'categoria_cargo', 'categoria_empresa', 
                           'categoria_requisito', 'franja_hora_entrevista', 'categoria_documentacion']
    numeric_features = ['tiene_contacto_empresa', 'dia_semana', 'mes', 'es_fin_semana']
    
    df_encoded = pd.get_dummies(df_procesado[categorical_features + numeric_features], 
                                columns=categorical_features, drop_first=True)
    
    for col in columnas_modelo:
        if col not in df_encoded.columns:
            df_encoded[col] = 0
    
    df_encoded = df_encoded[columnas_modelo]
    probabilidades = modelo.predict_proba(df_encoded)[:, 1]
    
    scores = ['Bajo' if p < 0.40 else 'Medio' if p < 0.55 else 'Alto' for p in probabilidades]
    
    df_resultado = df_procesado.copy()
    df_resultado['probabilidad_conversion'] = probabilidades
    df_resultado['score_priorizacion'] = scores
    
    print(f"✅ Predicciones: Alto={sum(1 for s in scores if s=='Alto')}, Medio={sum(1 for s in scores if s=='Medio')}, Bajo={sum(1 for s in scores if s=='Bajo')}")
    return df_resultado

def exportar_archivos(df_resultado):
    fecha_str = datetime.now().strftime('%Y%m%d')
    
    # Directorios
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'predicciones')
    analisis_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'analisis')
    vicidial_dir = r'C:\Users\JuanD.Ramirez\Documents\Colsubsidio\Colsubsidio_Agencia_Empleo\data\upload_vicidial'
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(analisis_dir, exist_ok=True)
    os.makedirs(vicidial_dir, exist_ok=True)
    
    columnas = ['tipo_de_gestion', 'codigo_vacante', 'cargo', 'empresa', 'fecha_entrevista', 'hora_entrevista',
                'no_documento', 'nombres', 'apellidos', 'phone', 'email', 'codigo_unico_vacante',
                'probabilidad_conversion', 'score_priorizacion']
    
    archivos = []
    
    # 1. Excel por score
    for score in ['Alto', 'Medio', 'Bajo']:
        df_score = df_resultado[df_resultado['score_priorizacion'] == score][columnas]
        if len(df_score) > 0:
            filepath = os.path.join(output_dir, f'COLSAGEM - Score_{score}_{fecha_str}.xlsx')
            df_score.to_excel(filepath, index=False, engine='openpyxl')
            archivos.append(filepath)
            print(f"✅ Excel: Score_{score} ({len(df_score)} registros)")
    
    # 2. Excel consolidado
    filepath_consol = os.path.join(analisis_dir, f'Consolidado_Analisis_{fecha_str}.xlsx')
    df_resultado[columnas].to_excel(filepath_consol, index=False, engine='openpyxl')
    archivos.append(filepath_consol)
    print(f"✅ Excel: Consolidado ({len(df_resultado)} registros)")
    
    # 3. CSV para Vicidial (3 archivos separados)
    for score in ['Alto', 'Medio', 'Bajo']:
        df_score_csv = df_resultado[df_resultado['score_priorizacion'] == score][columnas]
        if len(df_score_csv) > 0:
            filepath_csv = os.path.join(vicidial_dir, f'COLSAGEM - Score_{score}_{fecha_str}.csv')
            df_score_csv.to_csv(filepath_csv, index=False, encoding='utf-8')
            archivos.append(filepath_csv)
            print(f"✅ CSV: Score_{score} ({len(df_score_csv)} registros)")
    
    return archivos

def main():
    print("\n" + "="*70)
    print("🤖 PREDICCIÓN DIARIA")
    print("="*70 + "\n")
    
    modelo, columnas_modelo = cargar_modelo()
    if modelo is None: return
    
    engine = get_engine()
    if engine is None: return
    
    df_asignaciones = obtener_asignaciones_nuevas(engine)
    if df_asignaciones is None or len(df_asignaciones) == 0:
        print("⚠️ Sin asignaciones")
        return
    
    df_procesado = preprocesar_datos(df_asignaciones)
    df_resultado = generar_predicciones(df_procesado, modelo, columnas_modelo)
    archivos = exportar_archivos(df_resultado)
    
    print("\n" + "="*70)
    print(f"✅ COMPLETADO - {len(archivos)} archivos generados")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
