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
    public class Rotina9901
    {
        public int Codfilial { get; set; }
        public Int64? Produto { get; set; }
        public string Descricao { get; set; }
        public string Fornecedores { get; set; }

        public DataTable BuscaFornecedores()
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable fornecedores = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT CODFORNEC AS VALUE, CODFORNEC || ' - ' || FORNECEDOR AS LABEL FROM PCFORNEC ORDER BY FORNECEDOR");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(fornecedores);

                return fornecedores;
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

        public DataTable BuscaProdutos(Rotina9901 parametros)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            try
            {
                DataTable produtos = new DataTable();
                StringBuilder query = new StringBuilder();

                query.Append("SELECT PROD.CODPROD, PROD.CODPROD || ' - ' || PROD.DESCRICAO AS PRODUTO, PROD.EMBALAGEMMASTER, PROD.EMBALAGEM, ");
                query.Append("       NVL(EST.QTESTGER, 0) AS ESTERP, NVL(SUM(ESTWMS.QT), 0) AS ESTWMS, ");
                query.Append("       FORNEC.CODFORNEC || ' - ' || FORNEC.FORNECEDOR AS FORNECEDOR ");
                query.Append("  FROM PCPRODUT PROD INNER JOIN PCPRODFILIAL PF ON (PROD.CODPROD = PF.CODPROD)");
                query.Append("                     INNER JOIN PCFORNEC FORNEC ON (PROD.CODFORNEC = FORNEC.CODFORNEC) ");
                query.Append("                     LEFT OUTER JOIN PCEST EST ON (PROD.CODPROD = EST.CODPROD AND PF.CODFILIAL = EST.CODFILIAL) ");
                query.Append("                     INNER JOIN PCENDERECO EN ON (PF.CODFILIAL = EN.CODFILIAL) ");
                query.Append("                     LEFT OUTER JOIN PCESTENDERECO ESTWMS ON (EN.CODENDERECO = ESTWMS.CODENDERECO AND PROD.CODPROD = ESTWMS.CODPROD) ");
                query.Append($"WHERE PF.CODFILIAL = {parametros.Codfilial}");
                if (parametros.Produto != null)
                {
                query.Append($"  AND ((PROD.CODPROD = {parametros.Produto}) OR (PROD.CODAUXILIAR = {parametros.Produto}) OR (CODAUXILIAR2 = {parametros.Produto}))");
                }
                if (!string.IsNullOrEmpty(parametros.Descricao))
                {
                query.Append($"  AND UPPER(PROD.DESCRICAO) LIKE UPPER('%{parametros.Descricao}%')");
                }
                if (!string.IsNullOrEmpty(parametros.Fornecedores))
                {
                query.Append($"  AND FORNEC.CODFORNEC IN ({parametros.Fornecedores})");
                }
                query.Append(" GROUP BY PROD.CODPROD, PROD.CODPROD || ' - ' || PROD.DESCRICAO, PROD.DESCRICAO, PROD.EMBALAGEMMASTER, PROD.EMBALAGEM, ");
                query.Append("          NVL(EST.QTESTGER, 0), FORNEC.CODFORNEC || ' - ' || FORNEC.FORNECEDOR");                
                query.Append(" ORDER BY PROD.DESCRICAO");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(produtos);

                return produtos;
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
    }

    public class Rotina9901Edita
    {
        public int Codfilial { get; set; }
        public string DescFilial { get; set; }
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        // Dados do Master
        public Int64 Dun { get; set; }
        public string EmbalagemMaster { get; set; }
        public string CodigoFabrica { get; set; }
        public string UnidadeMaster { get; set; }
        public int Qtunitcx { get; set; }
        public double AlturaCx { get; set; }
        public double LarguraCx { get; set; }
        public double ComprimentoCx { get; set; }
        public double VolumeCxM3 { get; set; }
        public double PesoLiqCx { get; set; }
        public double PesoBrutoCx { get; set; }
        // Dados do Unit
        public Int64 Ean { get; set; }
        public string Embalagem { get; set; }
        public string Unidade { get; set; }
        public int Qtunit { get; set; }
        public double AlturaUn { get; set; }
        public double LarguraUn { get; set; }
        public double ComprimentoUn { get; set; }
        public double VolumeUnM3 { get; set; }
        public double PesoLiqUn { get; set; }
        public double PesoBrutoUn { get; set; }
        // Parametros Gerais
        public string TipoNorma { get; set; }
        public int Lastro { get; set; }
        public int Camada { get; set; }
        public int TotPalete { get; set; }
        public double PesoPalete { get; set; }
        public int PrazoValidade { get; set; }
        public double PercShelfLife { get; set; }
        public string EnderecamentoCubagem { get; set; }
        public int TipoEndereco { get; set; }
        public int TipoEstrutura { get; set; }
        public int CaracteristicaProduto { get; set; }
        public int TipoCarga { get; set; }
        public string AbastecePaleteFechado { get; set; }
        public string AbastecePaleteFechadoCx { get; set; }
        public int TipoProduto { get; set; }
        public string ExpedeCxFechada { get; set; }
        public string ValidaCpPkAbastecimento { get; set; }
        public string UsaControleValPk { get; set; }
        public string UsaControleVal { get; set; }
        public int NivelMinimoAbastecimento { get; set; }
        public int NivelMaximoAbastecimento { get; set; }
        public int RestricaoBlocado { get; set; }
        public string UsaPulmaoRegulador { get; set; }
        public string TipoVariacao { get; set; }
        public string TipoEstoque { get; set; }
        public string PesoVariavel { get; set; }
        public string Fracionado { get; set; }
        public string EstoquePorLote { get; set; }
        public string MultiplicadorConf { get; set; }
        public int? CodFunc { get; set; }
        // Picking
        public List<PickingProduto> Picking { get; set; }
        // Endereços de loja
        public List<EnderecoLoja> EnderecoLoja { get; set; }
        // Emb. Auxiliar
        public List<EmbalagemAuxiliarEBarraAlt> EmbalagemAuxliar { get; set; }
        // Cod. Barras Alternativo
        public List<EmbalagemAuxiliarEBarraAlt> CodBarrasAlternativo { get; set; }
        // Endereços do produto
        public List<EnderecosWms> EnderecosWms { get; set; }

        public Rotina9901Edita BuscaDadosProduto(int CodFilial, Int64 Produto)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder query = new StringBuilder();
            StringBuilder queryPicking = new StringBuilder();
            StringBuilder queryEndLoja = new StringBuilder();
            StringBuilder queryEmbAuxiliar = new StringBuilder();
            StringBuilder queryCodBarraAlt = new StringBuilder();
            StringBuilder queryEnderecos = new StringBuilder();

            Rotina9901Edita produto = new Rotina9901Edita();
            List<PickingProduto> listaPicking = new List<PickingProduto>();
            List<EnderecoLoja> listaEndLoja = new List<EnderecoLoja>();
            List<EmbalagemAuxiliarEBarraAlt> listaEmbAuxiliar = new List<EmbalagemAuxiliarEBarraAlt>();
            List<EmbalagemAuxiliarEBarraAlt> listaCodBarraAlt = new List<EmbalagemAuxiliarEBarraAlt>();
            List<EnderecosWms> listaEnderecos = new List<EnderecosWms>();

            try
            {
                query.Append("SELECT to_number(PF.CODFILIAL), PROD.CODPROD, PROD.DESCRICAO, ");
                // MASTER
                query.Append("       NVL(PROD.CODAUXILIAR2, 0) AS DUN, NVL(PROD.EMBALAGEMMASTER, ' ') AS EMBALAGEMMASTER, NVL(PROD.CODFAB, ' ') AS CODFAB, PROD.UNIDADEMASTER, PROD.QTUNITCX, ");
                query.Append("       NVL(PROD.ALTURAARM, 0) AS ALTURAARM, NVL(PROD.LARGURAARM, 0) AS LARGURAARM, NVL(PROD.COMPRIMENTOARM, 0) AS COMPRIMENTOARM, ROUND(NVL(PROD.VOLUMEARM, 0), 4) AS VOLUMEARM, ");
                query.Append("       NVL(PROD.PESOLIQMASTER, 0) AS PESOLIQMASTER, NVL(PROD.PESOBRUTOMASTER, 0) AS PESOBRUTOMASTER, ");
                // UNIT
                query.Append("       NVL(PROD.CODAUXILIAR, 0) AS EAN, PROD.EMBALAGEM, PROD.UNIDADE, PROD.QTUNIT, NVL(PROD.ALTURAM3, 0) AS ALTURAM3, NVL(PROD.LARGURAM3, 0) AS LARGURAM3, ");
                query.Append("       NVL(PROD.COMPRIMENTOM3, 0) AS COMPRIMENTOM3, ROUND(NVL(PROD.VOLUME, 0), 4) AS VOLUME, NVL(PROD.PESOLIQ, 0) AS PESOLIQ, NVL(PROD.PESOBRUTO, 0) AS PESOBRUTO, ");
                // PARAMETROS
                query.Append("       NVL(PF.NORMAPALETE, 'C') AS NORMAPALETE, NVL(PF.LASTROPAL, 0) LASTROPAL, NVL(PF.ALTURAPAL, 0) AS ALTURAPAL, NVL(PF.QTTOTPAL, 0) AS QTTOTPAL, NVL(PF.PESOPALETE, 0) AS PESOPALETE,");
                query.Append("       PF.PRAZOVAL, PF.PERCTOLERANCIAVAL, PROD.ENDERECAMENTOCUBAGEM, ");
                query.Append("       NVL(PF.TIPOPALPUL, 1) AS TIPOPALPUL, NVL(PF.CODTIPOESTRUTURAPUL, 1) AS CODTIPOESTRUTURAPUL, NVL(PF.CODCARACPROD, 1) AS CODCARACPROD, NVL(PF.TIPOCARGA, 1) AS TIPOCARGA, ");
                query.Append("       NVL(PF.ABASTEPALETE, 'N') AS ABASTEPALETE, NVL(PF.ABASTEPALETECX, 'N') AS ABASTEPALETECX, NVL(PF.TIPOPROD, 0) AS TIPOPROD, ");
                query.Append("       NVL(PF.EXPEDECAIXAFECHADA, 'N') AS EXPEDECAIXAFECHADA, PF.VALIDACAPACIDADEPICKING, NVL(PF.ESTOQUEPORDTVALIDADEPK, 'N') AS ESTOQUEPORDTVALIDADEPK, ");
                query.Append("       NVL(PF.NIVELMINIMOARM, 0) AS NIVELMINIMOARM, NVL(PF.NIVELMAXIMOARM, 0) AS NIVELMAXIMOARM, NVL(PF.RESTRICAOBLOCADO, 0) AS RESTRICAOBLOCADO, NVL(PF.USAPULMAOREGULADOR, 'N') AS USAPULMAOREGULADOR, ");
                query.Append("       NVL(PF.TIPOVARIACAO, 'C') AS TIPOVARIACAO, PROD.TIPOESTOQUE, PROD.PESOVARIAVEL, NVL(PF.FRACIONADO, 'N') AS FRACIONADO, PROD.ESTOQUEPORLOTE, NVL(PROD.UTILIZAMULTIPLICADOR, 'S') AS UTILIZAMULTIPLICADOR, ");
                query.Append("       FIL.RAZAOSOCIAL, NVL(PROD.ESTOQUEPORDTVALIDADE, 'N') AS ESTOQUEPORDTVALIDADE");
                query.Append("  FROM PCPRODUT PROD INNER JOIN PCPRODFILIAL PF ON (PROD.CODPROD = PF.CODPROD) ");
                query.Append("                     INNER JOIN PCFILIAL FIL ON (PF.CODFILIAL = FIL.CODIGO) ");
                query.Append($"WHERE PF.CODFILIAL = {CodFilial}");
                query.Append($"  AND ((PROD.CODPROD = {Produto}) OR (PROD.CODAUXILIAR = {Produto}) OR (PROD.CODAUXILIAR2 = {Produto}))");

                exec.CommandText = query.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                if (reader.Read())
                {
                    produto.Codfilial                = reader.GetInt32(0);
                    produto.DescFilial               = reader.GetString(52);
                    produto.Codprod                  = reader.GetInt32(1);
                    produto.Descricao                = reader.GetString(2);
                    produto.Dun                      = reader.GetInt64(3);
                    produto.EmbalagemMaster          = reader.GetString(4);
                    produto.CodigoFabrica            = reader.GetString(5);
                    produto.UnidadeMaster            = reader.GetString(6);
                    produto.Qtunitcx                 = reader.GetInt16(7);
                    produto.AlturaCx                 = reader.GetDouble(8);
                    produto.LarguraCx                = reader.GetDouble(9);
                    produto.ComprimentoCx            = reader.GetDouble(10);
                    produto.VolumeCxM3               = reader.GetDouble(11);
                    produto.PesoLiqCx                = reader.GetDouble(12);
                    produto.PesoBrutoCx              = reader.GetDouble(13);
                    produto.Ean                      = reader.GetInt64(14);
                    produto.Embalagem                = reader.GetString(15);
                    produto.Unidade                  = reader.GetString(16);
                    produto.Qtunit                   = reader.GetInt16(17);
                    produto.AlturaUn                 = reader.GetDouble(18);
                    produto.LarguraUn                = reader.GetDouble(19);
                    produto.ComprimentoUn            = reader.GetDouble(20);
                    produto.VolumeUnM3               = reader.GetDouble(21);
                    produto.PesoLiqUn                = reader.GetDouble(22);
                    produto.PesoBrutoUn              = reader.GetDouble(23);
                    produto.TipoNorma                = reader.GetString(24);
                    produto.Lastro                   = reader.GetInt32(25);
                    produto.Camada                   = reader.GetInt32(26);
                    produto.TotPalete                = reader.GetInt32(27);
                    produto.PesoPalete               = reader.GetDouble(28);
                    produto.PrazoValidade            = reader.GetInt32(29);
                    produto.PercShelfLife            = reader.GetInt32(30);
                    produto.EnderecamentoCubagem     = reader.GetString(31);
                    produto.TipoEndereco             = reader.GetInt16(32);
                    produto.TipoEstrutura            = reader.GetInt16(33);
                    produto.CaracteristicaProduto    = reader.GetInt16(34);
                    produto.TipoCarga                = reader.GetInt16(35);
                    produto.AbastecePaleteFechado    = reader.GetString(36);
                    produto.AbastecePaleteFechadoCx  = reader.GetString(37);
                    produto.TipoProduto              = reader.GetInt16(38);
                    produto.ExpedeCxFechada          = reader.GetString(39);
                    produto.ValidaCpPkAbastecimento  = reader.GetString(40);
                    produto.UsaControleValPk         = reader.GetString(41);
                    produto.NivelMinimoAbastecimento = reader.GetInt32(42);
                    produto.NivelMaximoAbastecimento = reader.GetInt32(43);
                    produto.RestricaoBlocado         = reader.GetInt32(44);
                    produto.UsaPulmaoRegulador       = reader.GetString(45);
                    produto.TipoVariacao             = reader.GetString(46);
                    produto.TipoEstoque              = reader.GetString(47);
                    produto.PesoVariavel             = reader.GetString(48);
                    produto.Fracionado               = reader.GetString(49);
                    produto.EstoquePorLote           = reader.GetString(50);
                    produto.MultiplicadorConf        = reader.GetString(51);
                    produto.UsaControleVal           = reader.GetString(53);

                    queryPicking.Append("SELECT PF.CODPROD, PROD.DESCRICAO, to_number(PF.CODFILIAL), F.RAZAOSOCIAL, PK.TIPOENDERECO, TE.DESCRICAO, PK.TIPOESTRUTURA, TES.DESCRICAO, PK.TIPO, ");
                    queryPicking.Append("       PK.CODENDERECO, EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, PK.CAPACIDADE, ");
                    queryPicking.Append("       ROUND((PK.PONTOREPOSICAO / PK.CAPACIDADE * 100), 3) AS PERCREPOSICAO, PK.PONTOREPOSICAO,");
                    queryPicking.Append($"      CASE WHEN (SELECT NVL(VALOR,'N') FROM PCPARAMETROWMS WHERE NOME = 'TRANSFERENCIATROCAPICKINGOSPENDENTE' AND CODFILIAL = {CodFilial}) = 'S' ");
                    queryPicking.Append("            THEN 'S'");
                    queryPicking.Append("            ELSE (SELECT CASE WHEN NVL(QT, 0) > 0 AND (NVL(QTPENDENTRADA, 0) + NVL(QTPENDSAIDA, 0)) = 0 THEN 'S' ELSE 'N' END AS PODE_TRANSF");
                    queryPicking.Append("                    FROM PCESTENDERECO WHERE CODENDERECO = EN.CODENDERECO)");
                    queryPicking.Append("        END PERMITE_TRANSF,");
                    queryPicking.Append("       (SELECT CASE WHEN NVL(QT, 0) = 0 AND (NVL(QTPENDENTRADA, 0) + NVL(QTPENDSAIDA, 0)) = 0 THEN 'S' ELSE 'N' END AS PODE_EXCLUIR ");
                    queryPicking.Append("          FROM PCESTENDERECO WHERE CODENDERECO = EN.CODENDERECO) AS PERMITE_EXCLUIR");
                    queryPicking.Append("  FROM PCPRODUT PROD INNER JOIN PCPRODFILIAL PF ON (PROD.CODPROD = PF.CODPROD)");
                    queryPicking.Append("                     INNER JOIN PCFILIAL F ON (PF.CODFILIAL = F.CODIGO)");
                    queryPicking.Append("                     INNER JOIN PCPRODUTPICKING PK ON (PF.CODPROD = PK.CODPROD AND PF.CODFILIAL = PK.CODFILIAL)");
                    queryPicking.Append("                     INNER JOIN PCTIPOPAL TE ON (PK.TIPOENDERECO = TE.CODIGO)");
                    queryPicking.Append("                     INNER JOIN PCTIPOESTRUTURA TES ON (PK.TIPOESTRUTURA = TES.CODIGO)");
                    queryPicking.Append("                     INNER JOIN PCENDERECO EN ON (PK.CODENDERECO = EN.CODENDERECO) ");
                    queryPicking.Append($"WHERE PF.CODPROD = {produto.Codprod}");
                    queryPicking.Append($"  AND PF.CODFILIAL = {CodFilial}");
                    queryPicking.Append("   AND NVL(TE.ENDERECOLOJA, 'N') = 'N'");

                    exec.CommandText = queryPicking.ToString();
                    OracleDataReader picking = exec.ExecuteReader();

                    while(picking.Read())
                    {
                        PickingProduto pickingProduto = new PickingProduto
                        {
                            Codprod            = picking.GetInt32(0),
                            Descricao          = picking.GetString(1),
                            Codfilial          = picking.GetInt32(2),
                            DescFilial         = picking.GetString(3),
                            CodTipoEndereco    = picking.GetInt32(4),
                            DescTipoEndereco   = picking.GetString(5),
                            CodTipoEstrutura   = picking.GetInt32(6),
                            DescTipoEstrutura  = picking.GetString(7),
                            TipoPicking        = picking.GetString(8),
                            CodEndereco        = picking.GetInt32(9),
                            Deposito           = picking.GetInt32(10),
                            Rua                = picking.GetInt32(11),
                            Predio             = picking.GetInt32(12),
                            Nivel              = picking.GetInt32(13),
                            Apto               = picking.GetInt32(14),
                            Capacidade         = picking.GetInt32(15),
                            PercPontoReposicao = picking.GetInt32(16),
                            PontoReposicao     = picking.GetInt32(17),
                            PermiteTransfPk    = picking.GetString(18),
                            PermiteExcluirPk   = picking.GetString(19),
                        };

                        listaPicking.Add(pickingProduto);
                    }

                    produto.Picking = listaPicking;
                    
                    queryEndLoja.Append("SELECT PF.CODPROD, PROD.DESCRICAO, to_number(PF.CODFILIAL), F.RAZAOSOCIAL, PK.TIPOENDERECO, TE.DESCRICAO, ");
                    queryEndLoja.Append("       PK.CODENDERECO, EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, PK.CAPACIDADE, ");
                    queryEndLoja.Append("       ROUND((PK.PONTOREPOSICAO / PK.CAPACIDADE * 100), 3) AS PERCREPOSICAO, PK.PONTOREPOSICAO");
                    queryEndLoja.Append("  FROM PCPRODUT PROD INNER JOIN PCPRODFILIAL PF ON (PROD.CODPROD = PF.CODPROD)");
                    queryEndLoja.Append("                     INNER JOIN PCFILIAL F ON (PF.CODFILIAL = F.CODIGO)");
                    queryEndLoja.Append("                     INNER JOIN PCPRODUTPICKING PK ON (PF.CODPROD = PK.CODPROD AND PF.CODFILIAL = PK.CODFILIAL)");
                    queryEndLoja.Append("                     INNER JOIN PCTIPOPAL TE ON (PK.TIPOENDERECO = TE.CODIGO)");
                    queryEndLoja.Append("                     INNER JOIN PCENDERECO EN ON (PK.CODENDERECO = EN.CODENDERECO) ");
                    queryEndLoja.Append($"WHERE PF.CODPROD = {produto.Codprod}");
                    queryEndLoja.Append($"  AND PF.CODFILIAL = {CodFilial}");
                    queryEndLoja.Append($"  AND NVL(TE.ENDERECOLOJA, 'N') = 'S'");

                    exec.CommandText = queryEndLoja.ToString();
                    OracleDataReader endLoja = exec.ExecuteReader();

                    while(endLoja.Read())
                    {
                        EnderecoLoja enderecoLoja = new EnderecoLoja
                        {
                            Codprod            = endLoja.GetInt32(0),
                            Descricao          = endLoja.GetString(1),
                            Codfilial          = endLoja.GetInt32(2),
                            DescFilial         = endLoja.GetString(3),
                            CodTipoEndereco    = endLoja.GetInt32(4),
                            DescTipoEndereco   = endLoja.GetString(5),
                            CodEndereco        = endLoja.GetInt32(6),
                            Deposito           = endLoja.GetInt32(7),
                            Rua                = endLoja.GetInt32(8),
                            Predio             = endLoja.GetInt32(9),
                            Nivel              = endLoja.GetInt32(10),
                            Apto               = endLoja.GetInt32(11),
                            Capacidade         = endLoja.GetInt32(12),
                            PontoReposicao     = endLoja.GetInt32(13),
                            PercPontoReposicao = endLoja.GetInt32(14),
                        };

                        listaEndLoja.Add(enderecoLoja);
                    }

                    produto.EnderecoLoja = listaEndLoja;

                    queryEmbAuxiliar.Append("SELECT PF.CODPROD, PROD.DESCRICAO, to_number(PF.CODFILIAL), F.RAZAOSOCIAL, EMB.CODAUXILIAR, EMB.EMBALAGEM, EMB.UNIDADE, EMB.QTUNIT");
                    queryEmbAuxiliar.Append("  FROM PCPRODUT PROD INNER JOIN PCPRODFILIAL PF ON (PROD.CODPROD = PF.CODPROD)");
                    queryEmbAuxiliar.Append("                     INNER JOIN PCFILIAL F ON (PF.CODFILIAL = F.CODIGO)");
                    queryEmbAuxiliar.Append("                     INNER JOIN PCEMBALAGEM EMB ON (PF.CODPROD = EMB.CODPROD AND PF.CODFILIAL = EMB.CODFILIAL)");
                    queryEmbAuxiliar.Append($"WHERE PF.CODPROD = {produto.Codprod}");
                    queryEmbAuxiliar.Append($"  AND PF.CODFILIAL = {CodFilial}");

                    exec.CommandText = queryEmbAuxiliar.ToString();
                    OracleDataReader embAuxliar = exec.ExecuteReader();

                    while (embAuxliar.Read())
                    {
                        EmbalagemAuxiliarEBarraAlt embalagem = new EmbalagemAuxiliarEBarraAlt
                        {
                            Codprod    = embAuxliar.GetInt32(0),
                            Descricao  = embAuxliar.GetString(1),
                            Codfilial  = embAuxliar.GetInt32(2),
                            DescFilial = embAuxliar.GetString(3),
                            CodBarra   = embAuxliar.GetInt64(4),
                            Embalagem  = embAuxliar.GetString(5),
                            Unidade    = embAuxliar.GetString(6),
                            Qtunit     = embAuxliar.GetInt32(7),
                        };

                        listaEmbAuxiliar.Add(embalagem);
                    }

                    produto.EmbalagemAuxliar = listaEmbAuxiliar;

                    queryCodBarraAlt.Append("SELECT PF.CODPROD, PROD.DESCRICAO, to_number(PF.CODFILIAL), F.RAZAOSOCIAL, to_number(BALT.CODBARRAS) AS CODAUXILIAR, BALT.EMBALAGEM, BALT.UNIDADE, BALT.QTUNIT");
                    queryCodBarraAlt.Append("  FROM PCPRODUT PROD INNER JOIN PCPRODFILIAL PF ON (PROD.CODPROD = PF.CODPROD)");
                    queryCodBarraAlt.Append("                     INNER JOIN PCFILIAL F ON (PF.CODFILIAL = F.CODIGO)");
                    queryCodBarraAlt.Append("                     INNER JOIN PCWMSCODBARRAS BALT ON (PF.CODPROD = BALT.CODPRODUTO AND PF.CODFILIAL = BALT.CODFILIAL)");
                    queryCodBarraAlt.Append($"WHERE PF.CODPROD = {produto.Codprod}");
                    queryCodBarraAlt.Append($"  AND PF.CODFILIAL = {CodFilial}");

                    exec.CommandText = queryCodBarraAlt.ToString();
                    OracleDataReader codBarraAlt = exec.ExecuteReader();

                    while (codBarraAlt.Read())
                    {
                        EmbalagemAuxiliarEBarraAlt barraAlt = new EmbalagemAuxiliarEBarraAlt
                        {
                            Codprod    = codBarraAlt.GetInt32(0),
                            Descricao  = codBarraAlt.GetString(1),
                            Codfilial  = codBarraAlt.GetInt32(2),
                            DescFilial = codBarraAlt.GetString(3),
                            CodBarra   = codBarraAlt.GetInt64(4),
                            Embalagem  = codBarraAlt.GetString(5),
                            Unidade    = codBarraAlt.GetString(6),
                            Qtunit     = codBarraAlt.GetInt32(7),
                        };

                        listaCodBarraAlt.Add(barraAlt);
                    }

                    produto.CodBarrasAlternativo = listaCodBarraAlt;

                    queryEnderecos.Append("select en.codendereco, en.tipoender, en.deposito, en.rua, en.predio, en.nivel, en.apto, nvl(est.qt, 0) as qt");
                    queryEnderecos.Append("  from pcendereco en inner join pcestendereco est on (en.codendereco = est.codendereco)");
                    queryEnderecos.Append("                     inner join pcprodut prod on (est.codprod = prod.codprod)");
                    queryEnderecos.Append($" where prod.codprod = {produto.Codprod}");
                    queryEnderecos.Append($"   and en.codfilial = {CodFilial}");
                    queryEnderecos.Append(" order by decode(en.tipoender, 'AP', 'AAP'), en.deposito, en.rua, case when mod(en.rua, 2) = 1 then en.predio end asc, case when mod(en.rua, 2) = 0 then en.predio end desc, en.nivel, en.apto");

                    exec.CommandText = queryEnderecos.ToString();
                    OracleDataReader enderecosWms = exec.ExecuteReader();

                    while (enderecosWms.Read())
                    {
                        EnderecosWms enderecos = new EnderecosWms
                        {
                            CodEndereco  = enderecosWms.GetInt32(0),
                            TipoEndereco = enderecosWms.GetString(1),
                            Deposito     = enderecosWms.GetInt32(2),
                            Rua          = enderecosWms.GetInt32(3),
                            Predio       = enderecosWms.GetInt32(4),
                            Nivel        = enderecosWms.GetInt32(5),
                            Apto         = enderecosWms.GetInt32(6),
                            Qt           = enderecosWms.GetInt32(7),
                        };

                        listaEnderecos.Add(enderecos);
                    }

                    produto.EnderecosWms = listaEnderecos;

                    return produto;
                }
                else
                {
                    produto = new Rotina9901Edita();

                    return produto;
                }
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

        public string GravarAlteracoesCadastro(Rotina9901Edita dados)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleTransaction transacao = connection.BeginTransaction();
            OracleCommand exec = connection.CreateCommand();
            OracleCommand execProcedure = new OracleCommand("STP_CADASTRA_END_PK", connection);

            StringBuilder gravaDadosPCPRODUT = new StringBuilder();
            StringBuilder gravaDadosPCPRODFILIAL = new StringBuilder();
            string erroGravacao = "Dados gravados com sucesso!";

            exec.Transaction = transacao;

            try
            {
                gravaDadosPCPRODUT.Append("UPDATE PCPRODUT ");
                // Dados da Master
                gravaDadosPCPRODUT.Append($"  SET CODAUXILIAR2         = {dados.Dun},");
                gravaDadosPCPRODUT.Append($"      CODFAB               = '{dados.CodigoFabrica}',");
                gravaDadosPCPRODUT.Append($"      ALTURAARM            = {dados.AlturaCx.ToString().Replace(',','.')},");
                gravaDadosPCPRODUT.Append($"      LARGURAARM           = {dados.LarguraCx.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      COMPRIMENTOARM       = {dados.ComprimentoCx.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      VOLUMEARM            = {dados.VolumeCxM3.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      PESOLIQMASTER        = {dados.PesoLiqCx.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      PESOBRUTOMASTER      = {dados.PesoBrutoCx.ToString().Replace(',', '.')},");
                // Dados da Unidade
                gravaDadosPCPRODUT.Append($"      CODAUXILIAR          = {dados.Ean},");
                gravaDadosPCPRODUT.Append($"      ALTURAM3             = {dados.AlturaUn.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      LARGURAM3            = {dados.LarguraUn.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      COMPRIMENTOM3        = {dados.ComprimentoUn.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      VOLUME               = {dados.VolumeUnM3.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      PESOLIQ              = {dados.PesoLiqUn.ToString().Replace(',', '.')},");
                gravaDadosPCPRODUT.Append($"      PESOBRUTO            = {dados.PesoBrutoUn.ToString().Replace(',', '.')},");
                // Parametros
                gravaDadosPCPRODUT.Append($"      TIPOESTOQUE          = '{dados.TipoEstoque}',");
                gravaDadosPCPRODUT.Append($"      PESOVARIAVEL         = '{dados.PesoVariavel}',");
                gravaDadosPCPRODUT.Append($"      ENDERECAMENTOCUBAGEM = '{dados.EnderecamentoCubagem}',");
                gravaDadosPCPRODUT.Append($"      ESTOQUEPORDTVALIDADE = '{dados.UsaControleVal}',");
                gravaDadosPCPRODUT.Append($"      ESTOQUEPORLOTE       = '{dados.EstoquePorLote}',");
                gravaDadosPCPRODUT.Append($"      UTILIZAMULTIPLICADOR = '{dados.MultiplicadorConf}',");

                gravaDadosPCPRODUT.Append($"      CODFUNCULTALTER      = {dados.CodFunc},");
                gravaDadosPCPRODUT.Append("       CODROTINAULTALTER    = 9901,");
                gravaDadosPCPRODUT.Append("       DTULTALTER           = SYSDATE ");
                gravaDadosPCPRODUT.Append($"WHERE CODPROD = {dados.Codprod}");

                exec.CommandText = gravaDadosPCPRODUT.ToString();
                OracleDataReader gravaPcprodut = exec.ExecuteReader();

                gravaDadosPCPRODFILIAL.Append("UPDATE PCPRODFILIAL ");
                // Dados da Norma Palete
                gravaDadosPCPRODFILIAL.Append($"  SET LASTROPAL               = {dados.Lastro},");
                gravaDadosPCPRODFILIAL.Append($"      ALTURAPAL               = {dados.Camada},");
                gravaDadosPCPRODFILIAL.Append($"      QTTOTPAL                = {dados.TotPalete},");
                gravaDadosPCPRODFILIAL.Append($"      PESOPALETE              = {dados.PesoPalete.ToString().Replace(',', '.')},");
                // Dados da Validade
                gravaDadosPCPRODFILIAL.Append($"      PRAZOVAL                = {dados.PrazoValidade},");
                gravaDadosPCPRODFILIAL.Append($"      PERCTOLERANCIAVAL       = {dados.PercShelfLife},");
                // Parametros
                gravaDadosPCPRODFILIAL.Append($"      NORMAPALETE             = '{dados.TipoNorma}',");
                gravaDadosPCPRODFILIAL.Append($"      USAPULMAOREGULADOR      = '{dados.UsaPulmaoRegulador}',");
                gravaDadosPCPRODFILIAL.Append($"      TIPOVARIACAO            = '{dados.TipoVariacao}',");
                gravaDadosPCPRODFILIAL.Append($"      FRACIONADO              = '{dados.Fracionado}',");
                gravaDadosPCPRODFILIAL.Append($"      EXPEDECAIXAFECHADA      = '{dados.ExpedeCxFechada}',");
                gravaDadosPCPRODFILIAL.Append($"      TIPOPROD                = {dados.TipoProduto},");
                gravaDadosPCPRODFILIAL.Append($"      ABASTEPALETE            = '{dados.AbastecePaleteFechado}',");
                gravaDadosPCPRODFILIAL.Append($"      ABASTEPALETECX          = '{dados.AbastecePaleteFechadoCx}',");
                gravaDadosPCPRODFILIAL.Append($"      TIPOCARGA               = {dados.TipoCarga},");
                gravaDadosPCPRODFILIAL.Append($"      VALIDACAPACIDADEPICKING = '{dados.ValidaCpPkAbastecimento}',");
                gravaDadosPCPRODFILIAL.Append($"      ESTOQUEPORDTVALIDADEPK  = '{dados.UsaControleValPk}',");
                gravaDadosPCPRODFILIAL.Append($"      TIPOPALPUL              = {dados.TipoEndereco},");
                gravaDadosPCPRODFILIAL.Append($"      CODTIPOESTRUTURAPUL     = {dados.TipoEstrutura},");
                gravaDadosPCPRODFILIAL.Append($"      CODCARACPROD            = {dados.CaracteristicaProduto},");
                gravaDadosPCPRODFILIAL.Append($"      NIVELMINIMOARM          = {dados.NivelMinimoAbastecimento},");
                gravaDadosPCPRODFILIAL.Append($"      NIVELMAXIMOARM          = {dados.NivelMaximoAbastecimento},");

                gravaDadosPCPRODFILIAL.Append($"      CODFUNCULTALTER         = {dados.CodFunc},");
                gravaDadosPCPRODFILIAL.Append("       CODROTINAULTALTER       = 9901 ");
                gravaDadosPCPRODFILIAL.Append($"WHERE CODPROD   = {dados.Codprod}");
                gravaDadosPCPRODFILIAL.Append($"  AND CODFILIAL = {dados.Codfilial}");

                exec.CommandText = gravaDadosPCPRODFILIAL.ToString();
                OracleDataReader gravaPcprodfilial = exec.ExecuteReader();

                if (dados.Picking.Count > 0)
                {
                    if (dados.Picking.Count == 1 && (dados.Picking[0].CodEndereco == dados.Picking[0].CodEnderecoAnterior || dados.Picking[0].CodEnderecoAnterior == null) && dados.Picking[0].CodEndereco != 0)
                    {
                        StringBuilder deletaPk = new StringBuilder();
                        StringBuilder deletaPkEnd = new StringBuilder();
                        StringBuilder deletaPkEst = new StringBuilder();

                        deletaPkEnd.Append("UPDATE PCENDERECO SET SITUACAO = 'L' ");
                        deletaPkEnd.Append($"WHERE CODFILIAL = {dados.Picking[0].Codfilial} ");
                        deletaPkEnd.Append($"  AND TIPOENDER = 'AP' ");
                        deletaPkEnd.Append("   AND CODENDERECO IN (SELECT CODENDERECO FROM PCESTENDERECO ");
                        deletaPkEnd.Append($"                       WHERE CODPROD = {dados.Picking[0].Codprod} ");
                        deletaPkEnd.Append($"                         AND CODENDERECO <> {dados.Picking[0].CodEndereco})");

                        exec.CommandText = deletaPkEnd.ToString();
                        OracleDataReader deletaPkEND = exec.ExecuteReader();

                        deletaPkEst.Append("DELETE FROM PCESTENDERECO ");
                        deletaPkEst.Append($"WHERE CODPROD = {dados.Picking[0].Codprod} ");
                        deletaPkEst.Append("   AND CODENDERECO IN (SELECT CODENDERECO FROM PCENDERECO ");
                        deletaPkEst.Append($"                       WHERE CODFILIAL = {dados.Picking[0].Codfilial} ");
                        deletaPkEst.Append($"                         AND CODENDERECO <> {dados.Picking[0].CodEndereco}");
                        deletaPkEst.Append("                          AND TIPOENDER = 'AP')");

                        exec.CommandText = deletaPkEst.ToString();
                        OracleDataReader deletaPkEST = exec.ExecuteReader();

                        deletaPk.Append("DELETE FROM PCPRODUTPICKING ");
                        deletaPk.Append($"WHERE CODPROD = {dados.Picking[0].Codprod} ");
                        deletaPk.Append($"  AND CODFILIAL = {dados.Picking[0].Codfilial} ");
                        deletaPk.Append($"  AND CODENDERECO <> {dados.Picking[0].CodEndereco}");

                        exec.CommandText = deletaPk.ToString();
                        OracleDataReader deletaPK = exec.ExecuteReader();

                        dados.Picking.ForEach(pk =>
                        {
                            execProcedure = new OracleCommand("STP_CADASTRA_END_PK", connection)
                            {
                                CommandType = CommandType.StoredProcedure
                            };

                            execProcedure.Parameters.Add("PCODPROD", OracleDbType.Int32).Value = pk.Codprod;
                            execProcedure.Parameters.Add("PCODFILIAL", OracleDbType.Int32).Value = pk.Codfilial;
                            execProcedure.Parameters.Add("PCODENDERECO", OracleDbType.Int32).Value = pk.CodEndereco;
                            execProcedure.Parameters.Add("PTIPOPICKING", OracleDbType.Varchar2).Value = pk.TipoPicking;
                            execProcedure.Parameters.Add("PCAPACIDADE", OracleDbType.Int32).Value = pk.Capacidade;
                            execProcedure.Parameters.Add("PPONTOREPOSICAO", OracleDbType.Int32).Value = pk.PontoReposicao;
                            execProcedure.Parameters.Add("PCODTIPOESTRUTURA", OracleDbType.Int32).Value = pk.CodTipoEstrutura;
                            execProcedure.Parameters.Add("PCODTIPOENDERECO", OracleDbType.Int32).Value = pk.CodTipoEndereco;
                            execProcedure.Parameters.Add("PCODFUNC", OracleDbType.Int32).Value = dados.CodFunc;

                            OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 1000)
                            {
                                Direction = ParameterDirection.Output
                            };

                            execProcedure.Parameters.Add(resposta);

                            execProcedure.ExecuteNonQuery();

                            erroGravacao = resposta.Value.ToString();
                        });
                    } 
                    else 
                    {
                        if (dados.Picking[0].CodEndereco != 0)
                        {
                            dados.Picking.ForEach(pk =>
                            {
                                execProcedure = new OracleCommand("STP_CADASTRA_END_PK", connection)
                                {
                                    CommandType = CommandType.StoredProcedure
                                };

                                execProcedure.Parameters.Add("PCODPROD", OracleDbType.Int32).Value = pk.Codprod;
                                execProcedure.Parameters.Add("PCODFILIAL", OracleDbType.Int32).Value = pk.Codfilial;
                                execProcedure.Parameters.Add("PCODENDERECO", OracleDbType.Int32).Value = pk.CodEndereco;
                                execProcedure.Parameters.Add("PTIPOPICKING", OracleDbType.Varchar2).Value = pk.TipoPicking;
                                execProcedure.Parameters.Add("PCAPACIDADE", OracleDbType.Int32).Value = pk.Capacidade;
                                execProcedure.Parameters.Add("PPONTOREPOSICAO", OracleDbType.Int32).Value = pk.PontoReposicao;
                                execProcedure.Parameters.Add("PCODTIPOESTRUTURA", OracleDbType.Int32).Value = pk.CodTipoEstrutura;
                                execProcedure.Parameters.Add("PCODTIPOENDERECO", OracleDbType.Int32).Value = pk.CodTipoEndereco;
                                execProcedure.Parameters.Add("PCODFUNC", OracleDbType.Int32).Value = dados.CodFunc;

                                OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 1000)
                                {
                                    Direction = ParameterDirection.Output
                                };

                                execProcedure.Parameters.Add(resposta);

                                execProcedure.ExecuteNonQuery();

                                erroGravacao = resposta.Value.ToString();
                            });
                        }
                        else
                        {
                            StringBuilder deletaPk = new StringBuilder();
                            StringBuilder deletaPkEnd = new StringBuilder();
                            StringBuilder deletaPkEst = new StringBuilder();

                            deletaPkEnd.Append("UPDATE PCENDERECO SET SITUACAO = 'L' ");
                            deletaPkEnd.Append($"WHERE CODFILIAL = {dados.Codfilial} ");
                            deletaPkEnd.Append($"  AND TIPOENDER = 'AP' ");
                            deletaPkEnd.Append("   AND CODENDERECO IN (SELECT CODENDERECO FROM PCESTENDERECO ");
                            deletaPkEnd.Append($"                       WHERE CODPROD = {dados.Codprod})");

                            exec.CommandText = deletaPkEnd.ToString();
                            OracleDataReader deletaPkEND = exec.ExecuteReader();

                            deletaPkEst.Append("DELETE FROM PCESTENDERECO ");
                            deletaPkEst.Append($"WHERE CODPROD = {dados.Codprod} ");
                            deletaPkEst.Append("   AND CODENDERECO IN (SELECT CODENDERECO FROM PCENDERECO ");
                            deletaPkEst.Append($"                       WHERE CODFILIAL = {dados.Codfilial}");
                            deletaPkEst.Append($"                         AND TIPOENDER = 'AP')");

                            exec.CommandText = deletaPkEst.ToString();
                            OracleDataReader deletaPkEST = exec.ExecuteReader();

                            deletaPk.Append("DELETE FROM PCPRODUTPICKING ");
                            deletaPk.Append($"WHERE CODPROD = {dados.Codprod} ");
                            deletaPk.Append($"  AND CODFILIAL = {dados.Codfilial} ");

                            exec.CommandText = deletaPk.ToString();
                            OracleDataReader deletaPK = exec.ExecuteReader();

                            erroGravacao = "Picking cadastrado com sucesso!";
                        }
                    }
                }

                if (dados.EnderecoLoja.Count > 0)
                {
                    StringBuilder deletaEndLoja = new StringBuilder();

                    deletaEndLoja.Append($"DELETE FROM PCPRODUTPICKING WHERE CODPROD = {dados.Codprod} AND CODFILIAL = {dados.Codfilial} ");
                    deletaEndLoja.Append("  AND CODENDERECO IN (SELECT CODENDERECO FROM PCTIPOPAL WHERE ENDERECOLOJA = 'S')");

                    exec.CommandText = deletaEndLoja.ToString();
                    OracleDataReader deletaEL = exec.ExecuteReader();

                    dados.EnderecoLoja.ForEach(endLoja =>
                    {
                        StringBuilder insereEndLoja = new StringBuilder();

                        insereEndLoja.Append("INSERT INTO PCPRODUTPICKING (CODPROD, CODFILIAL, CODENDERECO, TIPO, CAPACIDADE, PONTOREPOSICAO, TIPOESTRUTURA, TIPOENDERECO, CODENDERECOPTL)");
                        insereEndLoja.Append($"    VALUES ({endLoja.Codprod}, {endLoja.Codfilial}, {endLoja.CodEndereco}, NULL, {endLoja.Capacidade}, {endLoja.PontoReposicao}, NULL, {endLoja.CodTipoEndereco}, NULL)");

                        exec.CommandText = insereEndLoja.ToString();
                        OracleDataReader insereLJ = exec.ExecuteReader();
                    });
                }

                if (dados.EmbalagemAuxliar.Count > 0)
                {
                    StringBuilder deletaEmbAux = new StringBuilder();

                    deletaEmbAux.Append($"DELETE FROM PCEMBALAGEM WHERE CODPROD = {dados.Codprod} AND CODFILIAL = {dados.Codfilial}");
                    
                    exec.CommandText = deletaEmbAux.ToString();
                    OracleDataReader deletaEA = exec.ExecuteReader();

                    dados.EmbalagemAuxliar.ForEach(embAux =>
                    {
                        StringBuilder insereEmbAux = new StringBuilder();

                        insereEmbAux.Append("INSERT INTO PCEMBALAGEM (CODFILIAL, CODAUXILIAR, CODPROD, EMBALAGEM, UNIDADE, QTUNIT) ");
                        insereEmbAux.Append($"    VALUES ({embAux.Codfilial}, {embAux.CodBarra}, {embAux.Codprod}, '{embAux.Embalagem}', '{embAux.Unidade}', {embAux.Qtunit})");

                        exec.CommandText = insereEmbAux.ToString();
                        OracleDataReader insereEA = exec.ExecuteReader();
                    });
                }

                if (dados.CodBarrasAlternativo.Count > 0)
                {
                    StringBuilder deletaCodAlt = new StringBuilder();

                    deletaCodAlt.Append($"DELETE FROM PCWMSCODBARRAS WHERE CODPRODUTO = {dados.Codprod} AND CODFILIAL = {dados.Codfilial}");

                    exec.CommandText = deletaCodAlt.ToString();
                    OracleDataReader deletaCA = exec.ExecuteReader();

                    dados.CodBarrasAlternativo.ForEach(codAlt =>
                    {
                        StringBuilder insereCodAlt = new StringBuilder();

                        insereCodAlt.Append("INSERT INTO PCWMSCODBARRAS (CODFILIAL, CODBARRAS, CODPRODUTO, EMBALAGEM, UNIDADE, QTUNIT, TIPO) ");
                        insereCodAlt.Append($"VALUES ({codAlt.Codfilial}, {codAlt.CodBarra}, {codAlt.Codprod}, '{codAlt.Embalagem}', '{codAlt.Unidade}', {codAlt.Qtunit}, NULL)");

                        exec.CommandText = insereCodAlt.ToString();
                        OracleDataReader insereCA = exec.ExecuteReader();
                    });
                }

                if (erroGravacao == "Picking cadastrado com sucesso!" || erroGravacao.IndexOf("O.S. de transferência gerada com sucesso. Número: ") > 1 || erroGravacao == "Dados gravados com sucesso!")
                {
                    transacao.Commit();
                    return "Dados gravados com sucesso!";
                } 
                else
                {
                    transacao.Rollback();
                    return erroGravacao;
                }                
            }
            catch (Exception ex)
            {
                if (connection.State == ConnectionState.Open)
                {
                    transacao.Rollback();
                    connection.Close();

                    throw new Exception(ex.ToString());
                }

                transacao.Rollback();
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

        public DataTable ListaEnderecos(DTOPesquisaEndereco dados)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable enderecos = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT EN.CODENDERECO, EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, ");
                query.Append("       EN.TIPOPAL AS \"codTipoEndereco\", TE.DESCRICAO AS DESCENDERECO, EN.CODESTRUTURA AS \"codTipoEstrutura\", TES.DESCRICAO AS DESCESTRUTURA, ");
                query.Append("       DECODE(EN.STATUS, 'N', 'NORMAL', 'A', 'AVARIA', 'F', 'FALTA', 'E', 'EXESSO', 'C', 'CROSS', 'NÃO INF.') STATUS");
                query.Append("  FROM PCENDERECO EN INNER JOIN PCTIPOPAL TE ON (EN.TIPOPAL = TE.CODIGO)");
                query.Append("                     INNER JOIN PCTIPOESTRUTURA TES ON (EN.CODESTRUTURA = TES.CODIGO) ");
                query.Append($"WHERE EN.CODFILIAL = {dados.CodFilial}");
                query.Append("   AND NVL(EN.BLOQUEIO, 'N') = 'N'");
                query.Append("   AND EN.TIPOENDER = 'AP'");
                query.Append("   AND EN.SITUACAO = 'L'");
                query.Append($"  AND EN.TIPOPAL = {dados.TipoEndereco}");
                query.Append($"  AND EN.CODESTRUTURA = {dados.TipoEstrutura}");
                if (dados.CodEndereco != null && dados.CodEndereco != 0)
                {
                query.Append($"  AND EN.CODENDERECO = {dados.CodEndereco}");
                }
                if (dados.Rua != null && dados.Rua != 0)
                {
                    query.Append($"  AND EN.RUA = {dados.Rua}");
                }
                if (dados.Predio != null && dados.Predio != 0)
                {
                    query.Append($"  AND EN.PREDIO = {dados.Predio}");
                }
                if (dados.Nivel != null && dados.Nivel != 0)
                {
                    query.Append($"  AND EN.NIVEL = {dados.Nivel}");
                }
                if (dados.Apto != null && dados.Apto != 0)
                {
                    query.Append($"  AND EN.APTO = {dados.Apto}");
                }
                query.Append(" ORDER BY EN.RUA, CASE WHEN MOD(EN.RUA, 2) = 1 THEN EN.PREDIO END ASC, CASE WHEN MOD(EN.RUA, 2) = 0 THEN EN.PREDIO END DESC, EN.NIVEL, EN.APTO");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(enderecos);

                return enderecos;
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

        public DataTable ListaEnderecosLoja(int codFilial)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable enderecos = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT EN.CODENDERECO, EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, ");
                query.Append("       EN.TIPOPAL, TE.DESCRICAO AS DESCENDERECO, EN.CODESTRUTURA, TES.DESCRICAO AS DESCESTRUTURA, ");
                query.Append("       DECODE(EN.STATUS, 'N', 'NORMAL', 'A', 'AVARIA', 'F', 'FALTA', 'E', 'EXESSO', 'C', 'CROSS', 'NÃO INF.') STATUS");
                query.Append("  FROM PCENDERECO EN INNER JOIN PCTIPOPAL TE ON (EN.TIPOPAL = TE.CODIGO)");
                query.Append("                     INNER JOIN PCTIPOESTRUTURA TES ON (EN.CODESTRUTURA = TES.CODIGO) ");
                query.Append($"WHERE EN.CODFILIAL = {codFilial}");
                query.Append("   AND NVL(EN.BLOQUEIO, 'N') = 'N'");
                query.Append("   AND NVL(TE.ENDERECOLOJA, 'N') = 'S'");
                query.Append("   AND NVL(EN.SITUACAO, 'L') = 'L'");
                query.Append(" ORDER BY EN.RUA, CASE WHEN MOD(EN.RUA, 2) = 1 THEN EN.PREDIO END ASC, CASE WHEN MOD(EN.RUA, 2) = 0 THEN EN.PREDIO END DESC, EN.NIVEL, EN.APTO");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(enderecos);

                return enderecos;
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

        public DataTable ListaEnderecosLivres(int codFilial)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable enderecos = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT EN.CODENDERECO, EN.DEPOSITO, EN.RUA, EN.PREDIO, EN.NIVEL, EN.APTO, ");
                query.Append("       EN.TIPOPAL AS \"codTipoEndereco\", TE.DESCRICAO AS DESCENDERECO, EN.CODESTRUTURA AS \"codTipoEstrutura\", TES.DESCRICAO AS DESCESTRUTURA, ");
                query.Append("       DECODE(EN.STATUS, 'N', 'NORMAL', 'A', 'AVARIA', 'F', 'FALTA', 'E', 'EXESSO', 'C', 'CROSS', 'NÃO INF.') STATUS");
                query.Append("  FROM PCENDERECO EN INNER JOIN PCTIPOPAL TE ON (EN.TIPOPAL = TE.CODIGO)");
                query.Append("                     INNER JOIN PCTIPOESTRUTURA TES ON (EN.CODESTRUTURA = TES.CODIGO) ");
                query.Append($"WHERE EN.CODFILIAL = {codFilial}");
                query.Append("   AND NVL(EN.BLOQUEIO, 'N') = 'N'");
                query.Append("   AND EN.TIPOENDER = 'AP'");
                query.Append("   AND NVL(TE.ENDERECOLOJA, 'N') = 'N'");
                query.Append("   AND NVL(EN.SITUACAO, 'L') = 'L'");
                query.Append(" ORDER BY EN.RUA, CASE WHEN MOD(EN.RUA, 2) = 1 THEN EN.PREDIO END ASC, CASE WHEN MOD(EN.RUA, 2) = 0 THEN EN.PREDIO END DESC, EN.NIVEL, EN.APTO");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(enderecos);

                return enderecos;
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
    }

    public class ListaDataProduto
    {
        public List<TipoEstrutura> TipoEstrutura { get; set; }
        public List<TipoEndereco> TipoEndereco { get; set; }
        public List<CaracteristicaProduto> CaracteristicaProduto { get; set; }
        public ListaDataProduto BuscaListaDataProduto()
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            StringBuilder queryTipoEndereco = new StringBuilder();
            StringBuilder queryTipoEstrutura = new StringBuilder();
            StringBuilder queryCaracProd = new StringBuilder();

            ListaDataProduto lista = new ListaDataProduto();

            List<TipoEndereco> listaTipoEndereco = new List<TipoEndereco>();
            List<TipoEstrutura> listaTipoEstrutura= new List<TipoEstrutura>();
            List<CaracteristicaProduto> listaCaracProd = new List<CaracteristicaProduto>();

            try {
                queryTipoEndereco.Append("SELECT CODIGO, DESCRICAO FROM PCTIPOPAL");

                exec.CommandText = queryTipoEndereco.ToString();
                OracleDataReader readerTipoEndereco = exec.ExecuteReader();

                while(readerTipoEndereco.Read())
                {
                    TipoEndereco endereco = new TipoEndereco
                    {
                        Codigo = readerTipoEndereco.GetInt32(0),
                        Descricao = readerTipoEndereco.GetString(1),
                    };

                    listaTipoEndereco.Add(endereco);
                }

                lista.TipoEndereco = listaTipoEndereco;

                queryTipoEstrutura.Append("SELECT CODIGO, DESCRICAO FROM PCTIPOESTRUTURA");

                exec.CommandText = queryTipoEstrutura.ToString();
                OracleDataReader readerTipoEstrutura = exec.ExecuteReader();

                while(readerTipoEstrutura.Read())
                {
                    TipoEstrutura estrutura = new TipoEstrutura
                    {
                        Codigo = readerTipoEstrutura.GetInt32(0),
                        Descricao = readerTipoEstrutura.GetString(1),
                    };

                    listaTipoEstrutura.Add(estrutura);
                }

                lista.TipoEstrutura = listaTipoEstrutura;

                queryCaracProd.Append("SELECT CODIGO, DESCRICAO FROM PCCARACPROD");

                exec.CommandText = queryCaracProd.ToString();
                OracleDataReader readerCaracProd = exec.ExecuteReader();

                while(readerCaracProd.Read())
                {
                    CaracteristicaProduto caracProd = new CaracteristicaProduto
                    {
                        Codigo = readerCaracProd.GetInt32(0),
                        Descricao = readerCaracProd.GetString(1),
                    };

                    listaCaracProd.Add(caracProd);
                }

                lista.CaracteristicaProduto = listaCaracProd;

                return lista;
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
    }

    public class PickingProduto
    {
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        public int Codfilial { get; set; }
        public string DescFilial { get; set; }
        public int CodTipoEndereco { get; set; }
        public string DescTipoEndereco { get; set; }
        public int CodTipoEstrutura { get; set; }
        public string DescTipoEstrutura { get; set; }
        public string TipoPicking { get; set; }
        public int CodEndereco { get; set; }
        public int? CodEnderecoAnterior { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public int Capacidade { get; set; }
        public double PercPontoReposicao { get; set; }
        public double PontoReposicao { get; set; }
        public string PermiteTransfPk { get; set; }
        public string PermiteExcluirPk { get; set; }
    }
    
    public class EnderecoLoja
    {
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        public int Codfilial { get; set; }
        public string DescFilial { get; set; }
        public int CodTipoEndereco { get; set; }
        public string DescTipoEndereco { get; set; }
        public int CodEndereco { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public int Capacidade { get; set; }
        public double PercPontoReposicao { get; set; }
        public double PontoReposicao { get; set; }
    }

    public class EmbalagemAuxiliarEBarraAlt
    {
        public int Codprod { get; set; }
        public string Descricao { get; set; }
        public int Codfilial { get; set; }
        public string DescFilial { get; set; }
        public Int64 CodBarra { get; set; }
        public string Embalagem { get; set; }
        public string Unidade { get; set; }
        public int Qtunit { get; set; }
    }

    public class EnderecosWms
    {
        public int CodEndereco { get; set; }
        public string TipoEndereco { get; set; }
        public int Deposito { get; set; }
        public int Rua { get; set; }
        public int Predio { get; set; }
        public int Nivel { get; set; }
        public int Apto { get; set; }
        public int Qt { get; set; }
    }

    public class TipoEstrutura
    {
        public int Codigo { get; set; }
        public string Descricao { get; set; }
    }
    
    public class TipoEndereco
    {
        public int Codigo { get; set; }
        public string Descricao { get; set; }
    }
    
    public class CaracteristicaProduto
    {
        public int Codigo { get; set; }
        public string Descricao { get; set; }
    }

    public class DTOPesquisaEndereco
    {
        public int CodFilial { get; set; }
        public int TipoEndereco { get; set; }
        public int TipoEstrutura { get; set; }
        public int? CodEndereco { get; set; }
        public int? Rua { get; set; }
        public int? Predio { get; set; }
        public int? Nivel { get; set; }
        public int? Apto { get; set; }
    }
}
