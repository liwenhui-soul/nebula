%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { nebula }
%define parser_class_name { WKTParser }
%lex-param { nebula::WKTScanner& scanner }
%parse-param { nebula::WKTScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { nebula::Geometry** geom }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include <cstddef>
#include "common/geo/io/Geometry.h"

namespace nebula {

class WKTScanner;

}

}

%code {
    #include "common/geo/io/wkt/WKTScanner.h"
    static int yylex(nebula::WKTParser::semantic_type* yylval,
                     nebula::WKTParser::location_type *yylloc,
                     nebula::WKTScanner& scanner);
}

%union {
    double                                  doubleVal;
    Geometry*                               geomVal;
    Point*                                  pointVal;
    LineString*                             lineVal;
    Polygon*                                polygonVal;
    Coordinate                              coordVal;
    std::vector<Coordinate>*                coordListVal;
    std::vector<std::vector<Coordinate>>*   coordListListVal;
}

/* destructors */
%destructor {} <doubleVal> <coordVal>
%destructor {} <geomVal>
%destructor { delete $$; } <*>

/* wkt shape type prefix */
%token KW_POINT KW_LINESTRING KW_POLYGON

/* symbols */
%token L_PAREN R_PAREN COMMA

/* token type specification */
%token <doubleVal> DOUBLE

%type <geomVal> geometry
%type <pointVal> point
%type <lineVal> linestring
%type <polygonVal> polygon
%type <coordVal> coordinate
%type <coordListVal> coordinate_list
%type <coordListListVal> coordinate_list_list

%define api.prefix {wkt}

%start geometry

%%

geometry
  : point {
    $$ = new Geometry(std::move(*$1));
    delete $1;
    *geom = $$;
  }
  | linestring {
    $$ = new Geometry(std::move(*$1));
    delete $1;
    *geom = $$;
  }
  | polygon {
    $$ = new Geometry(std::move(*$1));
    delete $1;
    *geom = $$;
  }
;

point
  : KW_POINT L_PAREN coordinate R_PAREN {
      $$ = new Point(std::move($3));
  }
  ;

linestring
  : KW_LINESTRING L_PAREN coordinate_list R_PAREN {
      if ($3->size() < 2) {
        delete $3;
        throw nebula::WKTParser::syntax_error(@3, "LineString must have at least 2 coordinates");
      }
      $$ = new LineString(std::move(*$3));
      delete $3;
  }
  ;

polygon
  : KW_POLYGON L_PAREN coordinate_list_list R_PAREN {
      for (size_t i = 0; i < $3->size(); ++i) {
        const auto &coordList = (*$3)[i];
        if (coordList.size() < 4) {
          delete $3;
          throw nebula::WKTParser::syntax_error(@3, "Polygon's LinearRing must have at least 4 coordinates");
        }
        if (coordList.front() != coordList.back()) {
          delete $3;
          throw nebula::WKTParser::syntax_error(@3, "Polygon's LinearRing must be closed");
        }
      }
      $$ = new Polygon(std::move(*$3));
      delete $3;
  }
  ;

coordinate
  : DOUBLE DOUBLE {
      if (!Coordinate::isValidLng($1)) {
        throw nebula::WKTParser::syntax_error(@1, "Longitude must be between -180 and 180 degrees");
      }
      if (!Coordinate::isValidLat($2)) {
        throw nebula::WKTParser::syntax_error(@2, "Latitude must be between -90 and 90 degrees");
      }
      $$.x = $1;
      $$.y = $2;
  }
  ;

coordinate_list
  : coordinate {
      $$ = new std::vector<Coordinate>();
      $$->emplace_back(std::move($1));
  }
  | coordinate_list COMMA coordinate {
      $$ = $1;
      $$->emplace_back(std::move($3));
  }
  ;

coordinate_list_list
  : L_PAREN coordinate_list R_PAREN {
      $$ = new std::vector<std::vector<Coordinate>>();
      $$->emplace_back(std::move(*$2));
      delete $2;
  }
  | coordinate_list_list COMMA L_PAREN coordinate_list R_PAREN {
      $$ = $1;
      $$->emplace_back(std::move(*$4));
      delete $4;
  }
  ;

%%

void nebula::WKTParser::error(const nebula::WKTParser::location_type& loc,
                                const std::string &msg) {
    std::ostringstream os;
    if (msg.empty()) {
        os << "syntax error";
    } else {
        os << msg;
    }

    auto *wkt = scanner.wkt();
    if (wkt == nullptr) {
        os << " at " << loc;
        errmsg = os.str();
        return;
    }

    auto begin = loc.begin.column > 0 ? loc.begin.column - 1 : 0;
    if ((loc.end.filename
        && (!loc.begin.filename
            || *loc.begin.filename != *loc.end.filename))
        || loc.begin.line < loc.end.line
        || begin >= wkt->size()) {
        os << " at " << loc;
    } else if (loc.begin.column < (loc.end.column ? loc.end.column - 1 : 0)) {
        uint32_t len = loc.end.column - loc.begin.column;
        if (len > 80) {
            len = 80;
        }
        os << " near `" << wkt->substr(begin, len) << "'";
    } else {
        os << " near `" << wkt->substr(begin, 8) << "'";
    }

    errmsg = os.str();
}

static int yylex(nebula::WKTParser::semantic_type* yylval,
                 nebula::WKTParser::location_type *yylloc,
                 nebula::WKTScanner& scanner) {
    auto token = scanner.yylex(yylval, yylloc);
    return token;
}

