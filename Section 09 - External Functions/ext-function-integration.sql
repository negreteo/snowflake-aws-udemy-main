use role accountadmin;
use schema "ECOMMERCE_DB"."ECOMMERCE_DEV";

create or replace api integration currency_conversion_int
  api_provider = aws_api_gateway
  api_aws_role_arn = 'arn:aws:iam::965642570530:role/iamr-on-dev-gbl-sflk-aws-external-001'
  api_allowed_prefixes = ('https://agi3xt979i.execute-api.us-east-1.amazonaws.com/production/on-currency-conversion-api-resource')
  enabled = true;
  
desc integration currency_conversion_int;

create or replace external function currency_conversion_external_function(from_currency varchar,to_currency varchar)
    returns variant
    api_integration = currency_conversion_int
    as 'https://agi3xt979i.execute-api.us-east-1.amazonaws.com/production/on-currency-conversion-api-resource';

select currency_conversion_external_function('USD','EUR');
select currency_conversion_external_function('USD','MXN');

select currency_conversion_external_function('USD','EUR')[0] as exchange_value;
select currency_conversion_external_function('USD','MXN')[0] as exchange_value;