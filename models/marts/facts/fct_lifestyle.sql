WITH stg AS (
    SELECT * FROM {{ ref('stg_lifestyle') }}
),

-- Dimensiones Principales
dim_exercise AS (
    SELECT * FROM {{ ref('dim_exercise') }}
),
dim_meal AS (
    SELECT * FROM {{ ref('dim_meal') }}
),
dim_workout_type AS (
    SELECT * FROM {{ ref('dim_workout_type') }}
),
dim_experience_level AS (
    SELECT * FROM {{ ref('dim_experience_level') }}
),
dim_gender AS (
    SELECT * FROM {{ ref('dim_gender') }}
)
-- ¡Nota que ya no necesitamos los CTEs para body_part, cooking_method, equipment, etc.!

SELECT
    -- Llave primaria
    stg.lifestyle_key,

    -- Llaves Foráneas (Foreign Keys) de las Dimensiones PRINCIPALES
    dim_exercise.dim_exercise_key,
    dim_meal.dim_meal_key,
    dim_workout_type.dim_workout_type_key,
    dim_experience_level.dim_experience_level_key,
    dim_gender.dim_gender_key,

    -- Métricas (Measures) y atributos degenerados
    stg.Edad,
    stg.Peso,
    stg.Altura,
    stg.Maximo_Pulsaciones,
    stg.Promedio_Pulsaciones,
    stg.Pulsaciones_Reposo,
    stg.Duracion_Sesion_Horas,
    stg.Calorias_Quemadas,
    stg.Porcentaje_Grasa,
    stg.Ingesta_Agua_Litros,
    stg.Frecuencia_Entrenamiento_Por_Dia,
    stg.Indice_Masa_Corporal,
    stg.Frecuencia_Comidas_Diarias,
    stg.Ejercicio_Fisico,
    stg.Carbohidratos,
    stg.Proteinas,
    stg.Grasas,
    stg.Calorias,
    stg.Azucar_Gramos,
    stg.Sodio_Miligramos,
    stg.Colesterol_Miligramos,
    stg.Tamano_Porcion_Gramos,
    stg.Series,
    stg.Repeticiones,
    stg.IMC_Calculado,
    stg.Calorias_de_Macronutrientes,
    stg.Porcentaje_Carbohidratos,
    stg.Proteina_por_kg,
    stg.Porcentaje_Reserva_FC,
    stg.Porcentaje_Maximo_FC,
    stg.Balance_Calorico,
    stg.Masa_Magra_kg,
    stg.Quema_Esperada

FROM
    stg
-- Uniones solo con las dimensiones principales
LEFT JOIN dim_exercise
    ON stg.Nombre_Ejercicio = dim_exercise.Nombre_Ejercicio
LEFT JOIN dim_meal
    ON stg.Nombre_Comida = dim_meal.Nombre_Comida
LEFT JOIN dim_workout_type
    ON stg.Tipo_Entrenamiento = dim_workout_type.Tipo_Entrenamiento
LEFT JOIN dim_experience_level
    ON stg.Nivel_Experiencia = dim_experience_level.Nivel_Experiencia
LEFT JOIN dim_gender
    ON stg.Genero = dim_gender.Genero