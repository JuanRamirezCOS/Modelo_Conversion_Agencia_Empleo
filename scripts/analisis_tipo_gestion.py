"""
Análisis por Tipo de Gestión - Diagnóstico Profundo
Evalúa si el modelo funciona mejor segmentando por tipo
"""

import sys
import os
import pandas as pd
import numpy as np
import pickle
import warnings
warnings.filterwarnings('ignore')

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from database import get_engine, query_to_dataframe

def cargar_datos_training():
    """Cargar datos de training para comparar"""
    engine = get_engine()
    query = """
    SELECT 
        tipo_de_gestion,
        categoria_empresa,
        categoria_cargo,
        conversion
    FROM tb_modelo_conversion_intermediacion
    """
    df = query_to_dataframe(query, engine)
    print(f"✅ Training data: {len(df):,} registros")
    return df

def cargar_query_sql(nombre_archivo):
    sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', nombre_archivo)
    with open(sql_path, 'r', encoding='utf-8') as f:
        return f.read()

def main():
    print("\n" + "="*50)
    print("🔍 ANÁLISIS POR TIPO DE GESTIÓN")
    print("="*50 + "\n")
    
    # 1. Cargar training data
    print("📦 Cargando datos históricos...")
    df_train = cargar_datos_training()
    
    # 2. Cargar datos de hoy
    print("📊 Cargando asignaciones de hoy...")
    engine = get_engine()
    query = cargar_query_sql('query_asignaciones_diarias.sql')
    df_hoy = query_to_dataframe(query, engine)
    print(f"✅ Hoy: {len(df_hoy):,} registros")
    
    # 3. Análisis de tipo de gestión
    print("\n" + "="*50)
    print("📊 DISTRIBUCIÓN DE TIPO DE GESTIÓN")
    print("="*50)
    
    print("\n🕐 En TRAINING (histórico):")
    train_tipos = df_train['tipo_de_gestion'].value_counts()
    train_tipos_pct = (train_tipos / len(df_train) * 100).round(1)
    for tipo, count in train_tipos.head(10).items():
        pct = train_tipos_pct[tipo]
        print(f"  {tipo}: {count:,} ({pct}%)")
    
    print("\n📅 HOY:")
    hoy_tipos = df_hoy['tipo_de_gestion'].value_counts()
    hoy_tipos_pct = (hoy_tipos / len(df_hoy) * 100).round(1)
    for tipo, count in hoy_tipos.items():
        pct = hoy_tipos_pct[tipo]
        print(f"  {tipo}: {count:,} ({pct}%)")
    
    # 4. Conversión por tipo en training
    print("\n" + "="*50)
    print("📈 TASA DE CONVERSIÓN POR TIPO (Training)")
    print("="*50)
    
    conversion_por_tipo = df_train.groupby('tipo_de_gestion').agg({
        'conversion': ['sum', 'count', 'mean']
    }).round(4)
    conversion_por_tipo.columns = ['Conversiones', 'Total', 'Tasa']
    conversion_por_tipo['Tasa_%'] = (conversion_por_tipo['Tasa'] * 100).round(2)
    conversion_por_tipo = conversion_por_tipo.sort_values('Tasa_%', ascending=False)
    
    print(conversion_por_tipo)
    
    # 5. Análisis de empresas por tipo HOY
    print("\n" + "="*50)
    print("🏢 EMPRESAS POR TIPO DE GESTIÓN (HOY)")
    print("="*50)
    
    for tipo in df_hoy['tipo_de_gestion'].unique()[:3]:
        print(f"\n📋 {tipo}:")
        empresas = df_hoy[df_hoy['tipo_de_gestion'] == tipo]['empresa'].value_counts().head(5)
        for emp, count in empresas.items():
            print(f"  {emp}: {count}")
    
    # 6. Comparación de mix empresa/cargo
    print("\n" + "="*50)
    print("⚖️ COMPARACIÓN TRAINING vs HOY")
    print("="*50)
    
    print("\n📊 Top 5 Empresas:")
    print("\nTraining:")
    train_emp = df_train['categoria_empresa'].value_counts().head(5)
    train_emp_pct = (train_emp / len(df_train) * 100).round(1)
    for emp, count in train_emp.items():
        print(f"  {emp}: {train_emp_pct[emp]}%")
    
    # Categorizar empresas de hoy (simplificado)
    def categorizar_empresa_simple(empresa):
        if pd.isna(empresa):
            return 'otras'
        empresa_upper = str(empresa).upper()
        if 'MANEJO TECNICO' in empresa_upper or 'DICO' in empresa_upper or 'ATENTO' in empresa_upper:
            return 'bpo_callcenter'
        elif 'PRODUCTIVIDAD' in empresa_upper or 'TEMPORIZAR' in empresa_upper or 'MANPOWER' in empresa_upper:
            return 'servicios_temporales'
        else:
            return 'otras'
    
    df_hoy['categoria_empresa_simple'] = df_hoy['empresa'].apply(categorizar_empresa_simple)
    
    print("\nHoy:")
    hoy_emp = df_hoy['categoria_empresa_simple'].value_counts().head(5)
    hoy_emp_pct = (hoy_emp / len(df_hoy) * 100).round(1)
    for emp, count in hoy_emp.items():
        print(f"  {emp}: {hoy_emp_pct[emp]}%")
    
    # 7. Análisis crítico
    print("\n" + "="*50)
    print("💡 ANÁLISIS CRÍTICO")
    print("="*50)
    
    # Calcular overlap de tipos
    tipos_train = set(df_train['tipo_de_gestion'].unique())
    tipos_hoy = set(df_hoy['tipo_de_gestion'].unique())
    tipos_comunes = tipos_train.intersection(tipos_hoy)
    
    print(f"\n✅ Tipos en común: {len(tipos_comunes)} de {len(tipos_hoy)}")
    print(f"   {', '.join(list(tipos_comunes)[:5])}")
    
    # 8. Recomendación basada en análisis
    print("\n" + "="*50)
    print("🎯 HALLAZGOS Y RECOMENDACIONES")
    print("="*50)
    
    # Calcular diferencia de distribución
    diff_bpo = abs(hoy_emp_pct.get('bpo_callcenter', 0) - train_emp_pct.get('bpo_callcenter', 0))
    
    print(f"\n📊 Diferencia en composición:")
    print(f"   - BPO: {diff_bpo:.1f} puntos de diferencia")
    
    if diff_bpo > 30:
        print("\n⚠️ PROBLEMA IDENTIFICADO:")
        print("   La composición de HOY es MUY diferente al training")
        print("   BPO domina hoy pero era minoría en training")
        print("\n💡 SOLUCIONES POSIBLES:")
        print("   1. Reentrenar SOLO con meses recientes (nov-dic)")
        print("   2. Modelo separado por tipo de gestión")
        print("   3. Calibración por segmento")
    else:
        print("\n✅ La composición es similar")
        print("   El problema puede ser otro (features insuficientes)")
    
    # 9. Test rápido: ¿Qué pasaría con solo datos recientes?
    print("\n" + "="*50)
    print("🔬 SIMULACIÓN: ¿Y si entrenamos solo con Nov-Dic?")
    print("="*50)
    
    # Filtrar últimos 2 meses del training
    df_train['fecha_asignacion'] = pd.to_datetime(df_train.get('fecha_asignacion', pd.Timestamp.now()))
    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=60)
    
    if 'fecha_asignacion' in df_train.columns:
        df_recent = df_train[df_train['fecha_asignacion'] >= cutoff_date]
        
        if len(df_recent) > 1000:
            print(f"\n📊 Datos recientes disponibles: {len(df_recent):,}")
            
            recent_emp = df_recent['categoria_empresa'].value_counts().head(5)
            recent_emp_pct = (recent_emp / len(df_recent) * 100).round(1)
            
            print("\nTop 5 empresas en últimos 60 días:")
            for emp, count in recent_emp.items():
                diff = recent_emp_pct[emp] - train_emp_pct.get(emp, 0)
                arrow = "📈" if diff > 0 else "📉"
                print(f"  {emp}: {recent_emp_pct[emp]}% {arrow} ({diff:+.1f}pp vs. histórico)")
            
            # Calcular si hay más BPO recientemente
            bpo_recent = recent_emp_pct.get('bpo_callcenter', 0)
            bpo_train = train_emp_pct.get('bpo_callcenter', 0)
            
            if bpo_recent > bpo_train + 10:
                print("\n✅ HALLAZGO IMPORTANTE:")
                print(f"   BPO ha aumentado recientemente ({bpo_recent}% vs. {bpo_train}% histórico)")
                print("   Reentrenar con datos recientes MEJORARÍA el modelo")
        else:
            print("\n⚠️ Pocos datos recientes disponibles")
    
    print("\n" + "="*50)
    print("✅ ANÁLISIS COMPLETADO")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
