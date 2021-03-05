using System;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EPCTIWebApi.Model;

namespace EPCTIWebApi.Model
{
    public class Rotina9909
    {
        public int CodProd { get; set; }
        public string Descricao { get; set; }
        public int Qtunitcx { get; set; }
        public int Lastro { get; set; }
        public int Camada { get; set; }
        public int Norma { get; set; }
        public List<EnderecosProduto> EnderecosProduto { get; set; }
        public List<EnderecosDisponiveis> EnderecosDisponiveis { get; set; }
        public Rotina9909 BuscaDadosProdutoTransferencia(int codFilial, int produto)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            Rotina9909 dados = new Rotina9909();

            StringBuilder queryProduto = new StringBuilder();
            StringBuilder queryEndProd = new StringBuilder();
            List<EnderecosProduto> listaEndProd = new List<EnderecosProduto>();
            StringBuilder queryEndDisp = new StringBuilder();
            List<EnderecosDisponiveis> listaEndDisp = new List<EnderecosDisponiveis>();

            try
            {
                queryProduto.Append("SELECT PROD.CODPROD, PROD.DESCRICAO, NVL(PF.LASTROPAL, 0) AS LASTRO, NVL(PF.ALTURAPAL, 0) AS CAMADA, NVL(PF.QTTOTPAL, 0) AS NORMA, PROD.QTUNITCX");
                queryProduto.Append("  FROM PCPRODUT PROD INNER JOIN PCPRODFILIAL PF ON(PROD.CODPROD = PF.CODPROD) ");
                queryProduto.Append($"WHERE PF.CODFILIAL = {codFilial}");
                queryProduto.Append($"  AND PROD.CODPROD = {produto}");

                exec.CommandText = queryProduto.ToString();
                OracleDataReader dadosProduto = exec.ExecuteReader();

                if (dadosProduto.Read())
                {
                    dados.CodProd = dadosProduto.GetInt32(0);
                    dados.Descricao = dadosProduto.GetString(1);
                    dados.Lastro = dadosProduto.GetInt32(2);
                    dados.Camada = dadosProduto.GetInt32(3);
                    dados.Norma = dadosProduto.GetInt32(4);
                    dados.Qtunitcx = dadosProduto.GetInt32(5);
                }

                queryEndProd.Append("SELECT CASE WHEN EN.TIPOENDER = 'AP' THEN 'Picking' ELSE 'Aéreo' END TIPOENDER, EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, ");
                queryEndProd.Append("       NVL(EST.QT, 0) + NVL(EST.QTPENDENTRADA, 0) - NVL(EST.QTPENDSAIDA, 0) - NVL(EST.QTBLOQUEADA, 0) AS QT, CASE WHEN NVL(EST.QTPENDENTRADA, 0) > 0 AND NVL(EST.QTPENDSAIDA, 0) > 0");
                queryEndProd.Append("                                   THEN 'T'");//PENDENCIA SAIDA E ENTRADA
                queryEndProd.Append("                                  WHEN NVL(EST.QTPENDENTRADA, 0) > 0 AND NVL(EST.QTPENDSAIDA, 0) = 0");
                queryEndProd.Append("                                   THEN 'E'");//PENDENCIA APENAS DE ENTRADA
                queryEndProd.Append("                                  WHEN NVL(EST.QTPENDENTRADA, 0) = 0 AND NVL(EST.QTPENDSAIDA, 0) > 0");
                queryEndProd.Append("                                   THEN 'S'"); //PENDENCIA APENAS DE SAIDA
                queryEndProd.Append("                                  WHEN NVL(EST.QTPENDENTRADA, 0) = 0 AND NVL(EST.QTPENDSAIDA, 0) = 0");
                queryEndProd.Append("                                   THEN 'N'"); //SEM NENHUMA PENDENCIA
                queryEndProd.Append("                              END PENDENCIA, NVL(TO_CHAR(EST.DTVAL, 'DD/MM/YYYY'), ' ') AS DTVAL, NVL(EST.CODIGOUMA, 0) AS CODIGOUMA,");
                queryEndProd.Append("       ROUND(NVL(PK.CAPACIDADE, 0) * NVL(PROD.QTUNITCX, 0), 0) AS CAPACIDADE, ROUND(NVL(PK.PONTOREPOSICAO, 0) * NVL(PROD.QTUNITCX, 0), 0) AS PONTOREP, EN.CODENDERECO, NVL(EST.QTBLOQUEADA, 0) AS QTBLOQUEADA");
                queryEndProd.Append("  FROM PCENDERECO EN INNER JOIN PCESTENDERECO EST ON (EN.CODENDERECO = EST.CODENDERECO)");
                queryEndProd.Append("                     INNER JOIN PCPRODUT PROD ON (EST.CODPROD = PROD.CODPROD)");
                queryEndProd.Append("                     LEFT OUTER JOIN PCPRODUTPICKING PK ON (EN.CODFILIAL = PK.CODFILIAL AND EN.CODENDERECO = PK.CODENDERECO AND EST.CODPROD = PK.CODPROD) ");
                queryEndProd.Append($"WHERE EN.CODFILIAL = {codFilial}");
                queryEndProd.Append($"  AND EST.CODPROD = {produto}");
                queryEndProd.Append("   AND EN.STATUS <> 'A'");
                queryEndProd.Append("ORDER BY EN.TIPOENDER DESC, EN.DEPOSITO, EN.RUA, CASE WHEN MOD(EN.RUA, 2) = 1 THEN EN.PREDIO END ASC, CASE WHEN MOD(EN.RUA, 2) = 0 THEN EN.PREDIO END DESC, EN.NIVEL, EN.APTO");

                exec.CommandText = queryEndProd.ToString();
                OracleDataReader endProd = exec.ExecuteReader();

                while (endProd.Read())
                {
                    EnderecosProduto enderecos = new EnderecosProduto
                    {
                        TipoEndereco = endProd.GetString(0),
                        Deposito = endProd.GetInt16(1),
                        Rua = endProd.GetInt16(2),
                        Predio = endProd.GetInt16(3),
                        Nivel = endProd.GetInt16(4),
                        Apto = endProd.GetInt16(5),
                        Qt = endProd.GetInt16(6),
                        Pendencia = endProd.GetString(7),
                        DataValidade = endProd.GetString(8),
                        CodigoUma = endProd.GetInt32(9),
                        Capacidade = endProd.GetInt32(10),
                        PontoRep = endProd.GetInt32(11),
                        CodEndereco = endProd.GetInt32(12),
                        QtBloqueada = endProd.GetInt32(13)
                    };

                    listaEndProd.Add(enderecos);
                }

                dados.EnderecosProduto = listaEndProd;

                queryEndDisp.Append("SELECT TAB.TIPOENDERECO, TAB.SITUACAO, TAB.DEPOSITO, TAB.RUA, TAB.PREDIO, TAB.NIVEL, TAB.APTO, TAB.CODENDERECO, TAB.TIPOESTRUTURA, TAB.TIPOENDER, TAB.QT ");
                queryEndDisp.Append("  FROM (SELECT TP.DESCRICAO AS TIPOENDERECO, CASE WHEN EN.SITUACAO = 'L' THEN 'LIVRE' ELSE 'OCUPADO' END SITUACAO, ");
                queryEndDisp.Append("               EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, EN.CODENDERECO, TP.DESCRICAO AS TIPOESTRUTURA, ");
                queryEndDisp.Append("               CASE WHEN EN.TIPOENDER = 'AP' THEN 'PICKING' ELSE 'AÉREO' END TIPOENDER, NVL(EST.QT, 0) + NVL(EST.QTPENDENTRADA, 0) - NVL(EST.QTPENDSAIDA, 0) - NVL(EST.QTBLOQUEADA, 0) AS QT");
                queryEndDisp.Append("          FROM PCENDERECO EN INNER JOIN PCTIPOESTRUTURA TE ON (EN.CODESTRUTURA = TE.CODIGO)");
                queryEndDisp.Append("                             INNER JOIN PCTIPOPAL TP ON (EN.TIPOPAL = TP.CODIGO)");
                queryEndDisp.Append("                             LEFT OUTER JOIN PCESTENDERECO EST ON (EN.CODENDERECO = EST.CODENDERECO)");
                queryEndDisp.Append($"        WHERE EN.CODFILIAL = {codFilial}");
                queryEndDisp.Append("           AND EN.SITUACAO = 'L'");
                queryEndDisp.Append("           AND EN.BLOQUEIO = 'N'");
                queryEndDisp.Append("           AND EN.TIPOENDER = 'AE'");
                queryEndDisp.Append("           AND EN.STATUS <> 'A'");


                queryEndDisp.Append("        UNION ALL");


                queryEndDisp.Append("        SELECT TP.DESCRICAO AS TIPOENDERECO, CASE WHEN EN.SITUACAO = 'L' THEN 'LIVRE' ELSE 'OCUPADO' END SITUACAO, ");
                queryEndDisp.Append("               EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, EN.CODENDERECO, TP.DESCRICAO AS TIPOESTRUTURA, ");
                queryEndDisp.Append("               CASE WHEN EN.TIPOENDER = 'AP' THEN 'PICKING' ELSE 'AÉREO' END TIPOENDER, NVL(EST.QT, 0) + NVL(EST.QTPENDENTRADA, 0) - NVL(EST.QTPENDSAIDA, 0) - NVL(EST.QTBLOQUEADA, 0) AS QT");
                queryEndDisp.Append("          FROM PCENDERECO EN INNER JOIN PCTIPOESTRUTURA TE ON (EN.CODESTRUTURA = TE.CODIGO)");
                queryEndDisp.Append("                             INNER JOIN PCTIPOPAL TP ON (EN.TIPOPAL = TP.CODIGO)");
                queryEndDisp.Append("                             LEFT OUTER JOIN PCESTENDERECO EST ON (EN.CODENDERECO = EST.CODENDERECO)");
                queryEndDisp.Append($"        WHERE EN.CODFILIAL = {codFilial}");
                queryEndDisp.Append("           AND EN.SITUACAO = 'O'");
                queryEndDisp.Append("           AND EN.STATUS <> 'A'");
                queryEndDisp.Append($"          AND EST.CODPROD = {produto}) TAB");
                queryEndDisp.Append(" WHERE TAB.DEPOSITO = 1");
                queryEndDisp.Append(" ORDER BY TAB.DEPOSITO, TAB.RUA, CASE WHEN MOD(TAB.RUA, 2) = 1 THEN TAB.PREDIO END ASC, CASE WHEN MOD(TAB.RUA, 2) = 0 THEN TAB.PREDIO END DESC, TAB.NIVEL, TAB.APTO");

                exec.CommandText = queryEndDisp.ToString();
                OracleDataReader endDisp = exec.ExecuteReader();

                while (endDisp.Read())
                {
                    EnderecosDisponiveis disponiveis = new EnderecosDisponiveis
                    {
                        DescricaoTipoEndereco = endDisp.GetString(0),
                        Situacao = endDisp.GetString(1),
                        Deposito = endDisp.GetInt16(2),
                        Rua = endDisp.GetInt16(3),
                        Predio = endDisp.GetInt16(4),
                        Nivel = endDisp.GetInt16(5),
                        Apto = endDisp.GetInt16(6),
                        CodEndereco = endDisp.GetInt32(7),
                        DescricaoEstrutura = endDisp.GetString(8),
                        TipoEndereco = endDisp.GetString(9),
                        QtEstDisp = endDisp.GetInt16(10),
                    };

                    listaEndDisp.Add(disponiveis);
                }

                dados.EnderecosDisponiveis = listaEndDisp;

                return dados;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.ToString());
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();
            }
        }
        public List<int> ValidaEnderecos(string enderecos, string produtos)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            List<int> listaEnderecos = new List<int>();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT EN.CODENDERECO");
                query.Append("  FROM PCENDERECO EN ");
                query.Append($"WHERE EN.CODENDERECO IN ({enderecos})");
                query.Append($"  AND NOT EXISTS (SELECT CODENDERECO FROM PCESTENDERECO WHERE CODENDERECO = EN.CODENDERECO AND CODPROD IN ({produtos}))");
                query.Append("   AND EN.SITUACAO = 'O'");

                exec.CommandText = query.ToString();
                OracleDataReader validacao = exec.ExecuteReader();

                while (validacao.Read())
                {
                    listaEnderecos.Add(validacao.GetInt32(0));
                }

                return listaEnderecos;
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                exec.Dispose();
                connection.Dispose();

                throw new Exception(ex.ToString());
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                exec.Dispose();
                connection.Dispose();
            }
        }
        public RetornoTransf TransfereEnderecos(List<TransferenciaEnderecos> lista)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleTransaction transacao = connection.BeginTransaction();

            OracleCommand exec = new OracleCommand("STP_TRANSF_PROD_ENDERECO", connection);

            OracleCommand buscaTransacaoOsWms = connection.CreateCommand();
            StringBuilder query = new StringBuilder();

            RetornoTransf respostaTransferencia = new RetornoTransf();

            buscaTransacaoOsWms.Transaction = transacao;

            respostaTransferencia.RetornoTransferencia = "Transferência realizada com sucesso.";

            int numtranswsm;
            int ordemServico;

            try
            {
                query.Append("SELECT SEQ_PROX_NUMTRANSWMS.NEXTVAL AS TRANSACAO, EPOCA.DFSEQ_PCMOVENDPEND.NEXTVAL AS OS FROM DUAL");

                buscaTransacaoOsWms.CommandText = query.ToString();
                OracleDataReader dadosWms = buscaTransacaoOsWms.ExecuteReader();

                if (dadosWms.Read())
                {
                    numtranswsm = dadosWms.GetInt32(0);

                    ordemServico = dadosWms.GetInt32(1);

                    respostaTransferencia.RetornoTransferencia += " Transação: " + numtranswsm + " O.S: " + ordemServico;

                    lista.ForEach(dados => {
                        exec = new OracleCommand("STP_TRANSF_PROD_ENDERECO", connection);

                        exec.CommandType = CommandType.StoredProcedure;

                        exec.Parameters.Add("PCODPROD", OracleDbType.Int32).Value = dados.Codprod;
                        exec.Parameters.Add("PCODFILIAL", OracleDbType.Int32).Value = dados.Codfilial;
                        exec.Parameters.Add("PCODENDERECO_ORIG", OracleDbType.Int32).Value = dados.CodEnderecoOrig; 
                        exec.Parameters.Add("PCODENDERECO_DEST", OracleDbType.Int32).Value = dados.CodEnderecoDest; 
                        exec.Parameters.Add("PQTTRANSF", OracleDbType.Int32).Value = dados.Qttransf; 
                        exec.Parameters.Add("PNUMTRANSWMS", OracleDbType.Int32).Value = numtranswsm;
                        exec.Parameters.Add("PNUMOS", OracleDbType.Int32).Value = ordemServico;
                        exec.Parameters.Add("PCODFUNC", OracleDbType.Int32).Value = dados.CodFunc; 


                        OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 1000)
                        {
                            Direction = ParameterDirection.Output
                        };

                        exec.Parameters.Add(resposta);

                        exec.ExecuteNonQuery();
                    });

                    transacao.Commit();

                    respostaTransferencia.NumTransWms = numtranswsm;

                    return respostaTransferencia;

                } else
                {
                    respostaTransferencia.RetornoTransferencia = "Não foi possível encontrar uma transação/O.S válida. Por favor tente mais tarde.";
                    respostaTransferencia.NumTransWms = 0;

                    return respostaTransferencia;
                }               
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    transacao.Rollback();
                    connection.Close();
                    throw;
                }

                transacao.Rollback();
                buscaTransacaoOsWms.Dispose();
                exec.Dispose();
                connection.Dispose();

                throw;
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }

                buscaTransacaoOsWms.Dispose();
                exec.Dispose();
                connection.Dispose();
            }
        }
    }

    public class EnderecosProduto
    {
        public string TipoEndereco { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public int Qt { get; set; }
        public string Pendencia { get; set; }
        public string DataValidade { get; set; }
        public int CodigoUma { get; set; }
        public int CodEndereco { get; set; }
        public int Capacidade { get; set; }
        public int PontoRep { get; set; }
        public int QtBloqueada { get; set; }
    }
    public class EnderecosDisponiveis
    {
        public string DescricaoTipoEndereco { get; set; }
        public string Situacao { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public string TipoEndereco { get; set; }
        public int CodEndereco { get; set; }
        public string DescricaoEstrutura { get; set; }
        public int QtEstDisp { get; set; }
    }
    public class TransferenciaEnderecos
    {
        public int Codprod { get; set; }
        public int Codfilial { get; set; }
        public int CodEnderecoOrig { get; set; }
        public int CodEnderecoDest { get; set; }
        public int Qttransf { get; set; }
        public int CodFunc { get; set; }
    }
    public class RetornoTransf
    {
        public string RetornoTransferencia { get; set; }
        public int NumTransWms { get; set; }
    }
}
