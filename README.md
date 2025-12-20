# 🎯 Modelo Predictivo de Conversión - Agencia de Empleo Colsubsidio

Modelo de clasificación para predecir qué registros de intermediación laboral se convertirán en colocaciones exitosas.

## 📊 Datos del Proyecto

- **Dataset**: 182,562 registros (agosto - diciembre 2025)
- **Tasa de conversión**: 46.96%
- **Fuentes**: 
  - `tb_asignacion_intermediacion_v2` (asignaciones)
  - `tb_informe_intermediacion` (seguimiento)
- **Tabla consolidada**: `tb_modelo_conversion_intermediacion`

## 🎯 Objetivo

Predecir si un registro de intermediación resultará en una conversión exitosa basándose en:
- Características de la vacante (cargo, empresa, número de vacantes)
- Requisitos profesionales
- Información de contacto y entrevista
- Historial de intentos de contacto

## 📁 Estructura del Proyecto

```
Modelo_Agencia_empleo/
├── .env                          # Credenciales de BD (no subir a git)
├── requirements.txt              # Dependencias Python
├── README.md                     # Este archivo
├── data/
│   └── processed/                # Datos procesados
├── notebooks/
│   └── 01_exploracion_inicial.ipynb
├── src/
│   └── database.py               # Módulo de conexión a BD
├── scripts/                      # Scripts de entrenamiento y predicción
├── sql/                          # Stored Procedures y queries
└── models/                       # Modelos entrenados (.pkl)
```

## 🚀 Instalación

```bash
pip install -r requirements.txt
```

## 🔧 Configuración

Crear archivo `.env` con:
```
DB_HOST=172.70.7.61
DB_PORT=3306
DB_USER=tu_usuario
DB_PASSWORD=tu_password
DB_NAME=bbdd_cos_bog_colsubsidio_agencia_empleo
```

## 📈 Variable Objetivo

**Conversion** (binaria):
- `1`: Conversión exitosa (contacto efectivo sin tipificaciones negativas)
- `0`: No conversión (no contacto o tipificaciones: LLMDES, LLADES, VOLLAM, CONTER, NUMPET, NUMEQU, LLAMUD, LLACOR)

## 🔍 Features Principales

### Categóricas:
- `categoria_empresa`: limpieza, servicios_temporales, seguridad, etc.
- `categoria_cargo`: limpieza_aseo, seguridad, bodega_logistica, produccion, etc.
- `categoria_requisito`: bachiller, tecnico, profesional, etc.
- `rango_vacantes`: 1-100, 100-200, 200-300, 300+
- `franja_hora_entrevista`: mañana, tarde, multiple, sin_hora

### Numéricas:
- `intentos`: Número de intentos de contacto
- `duracion_llamada`: Duración en minutos
- `contacto`, `gestionado`, `sin_gestion`, `no_contacto`, `efectividad`: Variables booleanas

## 📊 Flujo de Trabajo

1. **Consolidación de datos**: SP `sp_consolidar_datos_modelo_conversion()`
2. **Exploración**: Notebook `01_exploracion_inicial.ipynb`
3. **Feature Engineering**: (próximo notebook)
4. **Modelado**: XGBoost, Random Forest, LightGBM
5. **Evaluación**: AUC-ROC, Precision, Recall, F1
6. **Deployment**: Script de predicción diaria

## 👤 Autor

Juan David Ramírez - Colsubsidio
Fecha inicio: Diciembre 2025
