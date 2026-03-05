"""
Schemas das tabelas Delta para o projeto de logística de caminhões.
Compatível com PySpark no Databricks.
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DateType,
    IntegerType,
    DoubleType,
)


def get_transportadoras_schema() -> StructType:
    """Schema da tabela de transportadoras (empresas de transporte)."""
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("nome", StringType(), False),
            StructField("cnpj", StringType(), False),
            StructField("razao_social", StringType(), True),
            StructField("endereco", StringType(), True),
            StructField("cidade", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("cep", StringType(), True),
            StructField("telefone", StringType(), True),
            StructField("email", StringType(), True),
            StructField("data_cadastro", DateType(), True),
            StructField("ativo", BooleanType(), True),
        ]
    )


def get_motoristas_schema() -> StructType:
    """Schema da tabela de motoristas (vinculados a uma transportadora)."""
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("transportadora_id", StringType(), False),
            StructField("nome", StringType(), False),
            StructField("cpf", StringType(), False),
            StructField("cnh", StringType(), True),
            StructField("categoria_cnh", StringType(), True),
            StructField("data_nascimento", DateType(), True),
            StructField("telefone", StringType(), True),
            StructField("email", StringType(), True),
            StructField("data_cadastro", DateType(), True),
            StructField("ativo", BooleanType(), True),
        ]
    )


def get_embarcadores_schema() -> StructType:
    """Schema da tabela de embarcadores (quem contrata o frete)."""
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("nome", StringType(), False),
            StructField("cnpj", StringType(), False),
            StructField("razao_social", StringType(), True),
            StructField("endereco", StringType(), True),
            StructField("cidade", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("cep", StringType(), True),
            StructField("telefone", StringType(), True),
            StructField("email", StringType(), True),
            StructField("data_cadastro", DateType(), True),
            StructField("ativo", BooleanType(), True),
        ]
    )


def get_cargas_schema() -> StructType:
    """Schema da tabela de cargas (realizadas, disponíveis e futuras)."""
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("embarcador_id", StringType(), False),
            StructField("transportadora_id", StringType(), True),
            StructField("motorista_id", StringType(), True),
            StructField("tipo_carga", StringType(), False),
            StructField("composicao_veiculo", StringType(), False),
            StructField("caracteristica_veiculo", StringType(), False),
            StructField("embalagem", StringType(), False),
            StructField("origem_cidade", StringType(), True),
            StructField("origem_estado", StringType(), True),
            StructField("destino_cidade", StringType(), True),
            StructField("destino_estado", StringType(), True),
            StructField("data_prevista_coleta", DateType(), True),
            StructField("data_prevista_entrega", DateType(), True),
            StructField("data_entrega", DateType(), True),
            StructField("status", StringType(), False),
            StructField("peso_kg", DoubleType(), True),
            StructField("valor_frete", DoubleType(), True),
            StructField("data_criacao", DateType(), True),
        ]
    )
