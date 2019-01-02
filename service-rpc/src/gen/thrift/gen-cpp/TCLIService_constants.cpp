/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "TCLIService_constants.h"

namespace apache { namespace hive { namespace service { namespace rpc { namespace thrift {

const TCLIServiceConstants g_TCLIService_constants;

TCLIServiceConstants::TCLIServiceConstants() {
  PRIMITIVE_TYPES.insert((TTypeId::type)0);
  PRIMITIVE_TYPES.insert((TTypeId::type)1);
  PRIMITIVE_TYPES.insert((TTypeId::type)2);
  PRIMITIVE_TYPES.insert((TTypeId::type)3);
  PRIMITIVE_TYPES.insert((TTypeId::type)4);
  PRIMITIVE_TYPES.insert((TTypeId::type)5);
  PRIMITIVE_TYPES.insert((TTypeId::type)6);
  PRIMITIVE_TYPES.insert((TTypeId::type)7);
  PRIMITIVE_TYPES.insert((TTypeId::type)8);
  PRIMITIVE_TYPES.insert((TTypeId::type)9);
  PRIMITIVE_TYPES.insert((TTypeId::type)15);
  PRIMITIVE_TYPES.insert((TTypeId::type)16);
  PRIMITIVE_TYPES.insert((TTypeId::type)17);
  PRIMITIVE_TYPES.insert((TTypeId::type)18);
  PRIMITIVE_TYPES.insert((TTypeId::type)19);
  PRIMITIVE_TYPES.insert((TTypeId::type)20);
  PRIMITIVE_TYPES.insert((TTypeId::type)21);
  PRIMITIVE_TYPES.insert((TTypeId::type)22);

  COMPLEX_TYPES.insert((TTypeId::type)10);
  COMPLEX_TYPES.insert((TTypeId::type)11);
  COMPLEX_TYPES.insert((TTypeId::type)12);
  COMPLEX_TYPES.insert((TTypeId::type)13);
  COMPLEX_TYPES.insert((TTypeId::type)14);

  COLLECTION_TYPES.insert((TTypeId::type)10);
  COLLECTION_TYPES.insert((TTypeId::type)11);

  TYPE_NAMES.insert(std::make_pair((TTypeId::type)21, "INTERVAL_DAY_TIME"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)22, "TIMESTAMP WITH LOCAL TIME ZONE"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)0, "BOOLEAN"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)1, "TINYINT"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)2, "SMALLINT"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)3, "INT"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)4, "BIGINT"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)5, "FLOAT"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)6, "DOUBLE"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)7, "STRING"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)8, "TIMESTAMP"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)9, "BINARY"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)10, "ARRAY"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)11, "MAP"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)12, "STRUCT"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)13, "UNIONTYPE"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)15, "DECIMAL"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)16, "NULL"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)17, "DATE"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)18, "VARCHAR"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)19, "CHAR"));
  TYPE_NAMES.insert(std::make_pair((TTypeId::type)20, "INTERVAL_YEAR_MONTH"));

  CHARACTER_MAXIMUM_LENGTH = "characterMaximumLength";

  PRECISION = "precision";

  SCALE = "scale";

}

}}}}} // namespace

