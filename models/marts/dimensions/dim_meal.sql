WITH stg AS (
    -- 1. Obtenemos los atributos únicos por comida
    SELECT DISTINCT
        Nombre_Comida,
        Tiempo_Preparacion_Minutos,
        Tiempo_Coccion_Minutos,
        Calificacion AS Calificacion_Comida,
        
        -- Campos de enlace para las sub-dimensiones
        Tipo_Comida,
        Tipo_Dieta,
        Metodo_Coccion
    FROM 
        {{ ref('stg_lifestyle') }}
    WHERE 
        Nombre_Comida IS NOT NULL
),

-- 2. Traemos las dimensiones "copo de nieve"
dim_meal_type AS (
    SELECT * FROM {{ ref('dim_meal_type') }}
),
dim_diet_type AS (
    SELECT * FROM {{ ref('dim_diet_type') }}
),
dim_cooking_method AS (
    SELECT * FROM {{ ref('dim_cooking_method') }}
)

-- 3. Unimos todo
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg.Nombre_Comida']) }} AS dim_meal_key,
    stg.Nombre_Comida,
    stg.Tiempo_Preparacion_Minutos,
    stg.Tiempo_Coccion_Minutos,
    stg.Calificacion_Comida,

    -- Estas son las llaves foráneas que crean el "copo de nieve"
    dim_meal_type.dim_meal_type_key,
    dim_diet_type.dim_diet_type_key,
    dim_cooking_method.dim_cooking_method_key

FROM stg
LEFT JOIN dim_meal_type 
    ON stg.Tipo_Comida = dim_meal_type.Tipo_Comida
LEFT JOIN dim_diet_type 
    ON stg.Tipo_Dieta = dim_diet_type.Tipo_Dieta
LEFT JOIN dim_cooking_method 
    ON stg.Metodo_Coccion = dim_cooking_method.Metodo_Coccion