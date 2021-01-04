using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EPCTIWebApi.Model
{
    public class Usuario
    {
        public int Filial { get; set; }
        public int Codigo { get; set; }
        public string Senha { get; set; }
        public string Nome { get; set; }
        public string AcessoSistema { get; set; }
        public string PermiteAltDadosLogisticos { get; set; }
        public string Base { get; set; }
        public string Token { get; set; }
        public string Erro { get; set; }
        public string Warning { get; set; }
        public string MensagemErroWarning { get; set; }

        public Usuario validaUsuario(Usuario usuario)
        {
            StringBuilder query = new StringBuilder();

            try
            {
                OracleConnection con = DataBase.NovaConexao(usuario.Base);

                OracleCommand cmd = con.CreateCommand();

                query.Append("SELECT TO_NUMBER(PCEMPR.CODFILIAL) AS FILIAL, PCEMPR.MATRICULA AS CODIGO, ");
                query.Append("       CASE WHEN INSTR(PCEMPR.NOME, ' ') = 0 THEN TRIM(PCEMPR.NOME) ELSE TRIM(SUBSTR(PCEMPR.NOME,1,(INSTR(PCEMPR.NOME, ' ')))) END NOME, ");
                query.Append("       CASE WHEN NVL((SELECT COUNT(*) FROM PCCONTRO C INNER JOIN PCROTINA ROT ON (ROT.CODIGO = C.CODROTINA) ");
                query.Append("                       WHERE CODUSUARIO = PCEMPR.MATRICULA AND ROT.ROTINAWEB = 'S' AND ROT.ROTINA = 'WEB' AND C.ACESSO = 'S'), 0) > 0 THEN 'S' ELSE 'N' END AS ACESSO_SISTEMA, ");
                query.Append("       NVL((SELECT ACESSO FROM PCCONTROI WHERE PCCONTROI.CODCONTROLE = 1 AND PCCONTROI.CODROTINA = 9901 AND PCCONTROI.CODUSUARIO = PCEMPR.MATRICULA),'N') AS PERMITE_ALTERAR_DADOS_LOGISTICOS");
                query.Append("  FROM PCEMPR ");
                query.Append($"WHERE UPPER(PCEMPR.NOME_GUERRA) = UPPER('{ usuario.Nome }')");
                query.Append($"  AND DECRYPT(PCEMPR.SENHABD, PCEMPR.USUARIOBD) = UPPER('{ usuario.Senha }')");

                cmd.CommandText = query.ToString();
                OracleDataReader reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    if (reader.GetString(3) == "S")
                    {
                        usuario.Filial = reader.GetInt32(0);
                        usuario.Codigo = reader.GetInt32(1);
                        usuario.Senha = "";
                        usuario.Nome = reader.GetString(2);
                        usuario.AcessoSistema = reader.GetString(3);
                        usuario.PermiteAltDadosLogisticos = reader.GetString(4);
                        usuario.Erro = "N";
                        usuario.Warning = "N";

                        con.Close();

                        return usuario;
                    }
                    else {
                        usuario.Erro = "N";
                        usuario.Warning = "S";
                        usuario.MensagemErroWarning = "Usuário sem acesso ao sistema.";

                        con.Close();

                        return usuario;
                    }
                }
                else
                {
                    usuario.Erro = "N";
                    usuario.Warning = "S";
                    usuario.MensagemErroWarning = "Usuário/senha inválido.";

                    con.Close();

                    return usuario;
                }
            }
            catch (Exception ex)
            {
                usuario.Erro = "S";
                usuario.MensagemErroWarning = ex.Message;

                return usuario;
            }

        }
    }
}
