// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encodings

// Latin1_spanish_ci_RuneWeight returns the weight of a given rune based on its relational sort order from
// the `latin1_spanish_ci` collation.
func Latin1_spanish_ci_RuneWeight(r rune) int32 {
	weight, ok := latin1_spanish_ci_Weights[r]
	if ok {
		return weight
	} else {
		return 2147483647
	}
}

// latin1_spanish_ci_Weights contain a map from rune to weight for the `latin1_spanish_ci` collation. The
// map primarily contains mappings that have a random order. Mappings that fit into a sequential range (and are long
// enough) are defined in the calling function to save space.
var latin1_spanish_ci_Weights = map[rune]int32{
	0:    0,
	1:    1,
	2:    2,
	3:    3,
	4:    4,
	5:    5,
	6:    6,
	7:    7,
	8:    8,
	9:    9,
	10:   10,
	11:   11,
	12:   12,
	13:   13,
	14:   14,
	15:   15,
	16:   16,
	17:   17,
	18:   18,
	19:   19,
	20:   20,
	21:   21,
	22:   22,
	23:   23,
	24:   24,
	25:   25,
	26:   26,
	27:   27,
	28:   28,
	29:   29,
	30:   30,
	31:   31,
	32:   32,
	33:   33,
	34:   34,
	35:   35,
	36:   36,
	37:   37,
	38:   38,
	39:   39,
	40:   40,
	41:   41,
	42:   42,
	43:   43,
	44:   44,
	45:   45,
	46:   46,
	47:   47,
	48:   48,
	49:   49,
	50:   50,
	51:   51,
	52:   52,
	53:   53,
	54:   54,
	55:   55,
	56:   56,
	57:   57,
	58:   58,
	59:   59,
	60:   60,
	61:   61,
	62:   62,
	63:   63,
	64:   64,
	65:   65,
	97:   65,
	192:  65,
	193:  65,
	194:  65,
	195:  65,
	196:  65,
	197:  65,
	198:  65,
	224:  65,
	225:  65,
	226:  65,
	227:  65,
	228:  65,
	229:  65,
	230:  65,
	66:   66,
	98:   66,
	67:   67,
	99:   67,
	199:  67,
	231:  67,
	68:   68,
	100:  68,
	208:  68,
	240:  68,
	69:   69,
	101:  69,
	200:  69,
	201:  69,
	202:  69,
	203:  69,
	232:  69,
	233:  69,
	234:  69,
	235:  69,
	70:   70,
	102:  70,
	71:   71,
	103:  71,
	72:   72,
	104:  72,
	73:   73,
	105:  73,
	204:  73,
	205:  73,
	206:  73,
	207:  73,
	236:  73,
	237:  73,
	238:  73,
	239:  73,
	74:   74,
	106:  74,
	75:   75,
	107:  75,
	76:   76,
	108:  76,
	77:   77,
	109:  77,
	78:   78,
	110:  78,
	209:  79,
	241:  79,
	79:   80,
	111:  80,
	210:  80,
	211:  80,
	212:  80,
	213:  80,
	214:  80,
	216:  80,
	242:  80,
	243:  80,
	244:  80,
	245:  80,
	246:  80,
	248:  80,
	80:   81,
	112:  81,
	81:   82,
	113:  82,
	82:   83,
	114:  83,
	83:   84,
	115:  84,
	223:  85,
	84:   86,
	116:  86,
	85:   87,
	117:  87,
	217:  87,
	218:  87,
	219:  87,
	220:  87,
	249:  87,
	250:  87,
	251:  87,
	252:  87,
	86:   88,
	118:  88,
	87:   89,
	119:  89,
	88:   90,
	120:  90,
	89:   91,
	121:  91,
	221:  91,
	253:  91,
	255:  91,
	90:   92,
	122:  92,
	222:  93,
	254:  93,
	91:   94,
	92:   95,
	93:   96,
	94:   97,
	95:   98,
	96:   99,
	123:  100,
	124:  101,
	125:  102,
	126:  103,
	215:  104,
	247:  105,
	127:  106,
	8364: 107,
	129:  108,
	8218: 109,
	402:  110,
	8222: 111,
	8230: 112,
	8224: 113,
	8225: 114,
	710:  115,
	8240: 116,
	352:  117,
	8249: 118,
	338:  119,
	141:  120,
	381:  121,
	143:  122,
	144:  123,
	8216: 124,
	8217: 125,
	8220: 126,
	8221: 127,
	8226: 128,
	8211: 129,
	8212: 130,
	732:  131,
	8482: 132,
	353:  133,
	8250: 134,
	339:  135,
	157:  136,
	382:  137,
	376:  138,
	160:  139,
	161:  140,
	162:  141,
	163:  142,
	164:  143,
	165:  144,
	166:  145,
	167:  146,
	168:  147,
	169:  148,
	170:  149,
	171:  150,
	172:  151,
	173:  152,
	174:  153,
	175:  154,
	176:  155,
	177:  156,
	178:  157,
	179:  158,
	180:  159,
	181:  160,
	182:  161,
	183:  162,
	184:  163,
	185:  164,
	186:  165,
	187:  166,
	188:  167,
	189:  168,
	190:  169,
	191:  170,
}
