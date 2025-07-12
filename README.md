# ProyectoDataEngineering
Proyecto Final del Curso de Data Engineering

Distribución de Materiales de Construcción

Este proyecto implementa una arquitectura basada en Data Mesh para habilitar una fuente de la verdad confiable y escalable para los dominios clave del negocio. El objetivo principal es mejorar el acceso, calidad y gobernanza de los datos en una organización dedicada a la distribución de materiales de construcción.

1. Arquitectura y Enfoque

La solución propuesta se alinea con los principios de Data Mesh, donde cada dominio es responsable de la calidad y publicación de sus propios datos como productos. Se ha definido una arquitectura técnica que incorpora herramientas modernas del ecosistema de ingeniería de datos, asegurando:

- Desacoplamiento de dominios
- Pocesamiento distribuido
- Gobierno federado
- Escalabilidad horizontal

2. Componentes del Proyecto

El repositorio contiene los distintos elementos técnicos utilizados:

- Scripts: Transformaciones y lógica de negocio.
- DAGs (Airflow): Orquestación de pipelines de datos.
- Notebooks (Jupyter/Colab): Análisis exploratorios y pruebas.
- Esquemas y modelos: Estructuras de datos utilizadas para modelar los productos de datos.

3. Dominios cubiertos

Se han trabajado los siguientes dominios principales:
- Clientes
- Pedidos
- Despachos

4.Contribuciones

Este proyecto fue desarrollado como trabajo final colaborativo del curso de Data Engineering.