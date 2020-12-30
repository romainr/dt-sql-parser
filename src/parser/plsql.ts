import { InputStream, CommonTokenStream, Lexer } from 'antlr4';
import { PlSqlLexer } from '../lib/plsql/PlSqlLexer';
import { PlSqlParser } from '../lib/plsql/PlSqlParser';
export * from '../lib/plsql/PlSqlParserListener';
export * from '../lib/plsql/PlSqlParserVisitor';

import BasicParser from './common/BasicParser';

export default class PLSQLParser extends BasicParser {
    public createLexer(input: string): Lexer {
        const chars = new InputStream(input.toUpperCase());
        const lexer = <unknown> new PlSqlLexer(chars) as Lexer;
        return lexer;
    }
    public createParserFromLexer(lexer: Lexer) {
        const tokenStream = new CommonTokenStream(lexer);
        return new PlSqlParser(tokenStream);
    }
}
