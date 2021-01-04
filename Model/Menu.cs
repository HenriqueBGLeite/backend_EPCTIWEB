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
    public class Menu
    {
        public List<Modulo> ListaMenu { get; set; }

        public Menu BuscarRotinas(int matricula)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            Menu listaRotinas = new Menu();

            List<Modulo> ListaModulo = new List<Modulo>();
            List<SubModulo> ListaSubModulo;
            List<Rotina> ListaRotina;

            StringBuilder queryModulo = new StringBuilder(); ;
            StringBuilder querySubModulo;
            StringBuilder queryRotina;

            Int32 codmodulo;
            Int32 codsubmodulo;

            try
            {               
                queryModulo.Append("select distinct rot.codmodulo, mod.modulo as descmodulo ");
                queryModulo.Append("  from pcrotina rot inner join pcmodulo mod on (rot.codmodulo = mod.codmodulo)");
                queryModulo.Append("                    left outer join pccontro co on (rot.codigo = co.codrotina)");
                queryModulo.Append(" where rot.rotinaweb = 'S'");
                queryModulo.Append("   and rot.rotina = 'WEB'");
                queryModulo.Append("   and co.acesso = 'S'");
                queryModulo.Append($"  and co.codusuario = {matricula}");
                queryModulo.Append(" order by rot.codmodulo");

                exec.CommandText = queryModulo.ToString();
                OracleDataReader buscaModulos = exec.ExecuteReader();

                while (buscaModulos.Read())
                {
                    Modulo modulo = new Modulo
                    {
                        Codmodulo = buscaModulos.GetInt32(0),
                        Descmodulo = buscaModulos.GetString(1)
                    };

                    codmodulo = buscaModulos.GetInt32(0);

                    querySubModulo = new StringBuilder();

                    ListaSubModulo = new List<SubModulo>();

                    querySubModulo.Append("select distinct rot.codsubmodulo, sub.submodulo as descsubmodulo ");
                    querySubModulo.Append("  from pcrotina rot inner join pcmodulo mod on (rot.codmodulo = mod.codmodulo)");
                    querySubModulo.Append("                    inner join pcsubmodulo sub on(mod.codmodulo = sub.codmodulo and rot.codsubmodulo = sub.codsubmodulo)");
                    querySubModulo.Append("                    left outer join pccontro co on (rot.codigo = co.codrotina)");
                    querySubModulo.Append(" where rot.rotinaweb = 'S'");
                    querySubModulo.Append("   and co.acesso = 'S'");
                    querySubModulo.Append($"  and rot.codmodulo = {codmodulo}");
                    querySubModulo.Append($"  and co.codusuario = {matricula}");
                    querySubModulo.Append(" order by rot.codsubmodulo");

                    exec.CommandText = querySubModulo.ToString();
                    OracleDataReader buscaSubModulos = exec.ExecuteReader();

                    while (buscaSubModulos.Read())
                    {
                        SubModulo subModulo = new SubModulo
                        {
                            Codmodulo = buscaModulos.GetInt32(0),
                            Codsubmodulo = buscaSubModulos.GetInt32(0),
                            Descsubmodulo = buscaSubModulos.GetString(1)
                        };

                        codsubmodulo = buscaSubModulos.GetInt32(0);

                        queryRotina = new StringBuilder();

                        ListaRotina = new List<Rotina>();

                        queryRotina.Append("select rot.codigo as codrotina, rot.nomerotina as descrotina, nvl(rot.acao, '/') as rota, rot.codigo || ' - ' || rot.nomerotina  as nomeFiltro");
                        queryRotina.Append("  from pcrotina rot inner join pcmodulo mod on (rot.codmodulo = mod.codmodulo)");
                        queryRotina.Append("                    inner join pcsubmodulo sub on(mod.codmodulo = sub.codmodulo and rot.codsubmodulo = sub.codsubmodulo)");
                        queryRotina.Append("                    left outer join pccontro co on (rot.codigo = co.codrotina)");
                        queryRotina.Append(" where rot.rotinaweb = 'S'");
                        queryRotina.Append("   and co.acesso = 'S'");
                        queryRotina.Append($"  and rot.codmodulo = {codmodulo}");
                        queryRotina.Append($"  and rot.codsubmodulo = {codsubmodulo}");
                        queryRotina.Append($"  and co.codusuario = {matricula}");
                        queryRotina.Append(" order by rot.codigo");

                        exec.CommandText = queryRotina.ToString();
                        OracleDataReader buscaRotinas = exec.ExecuteReader();

                        while (buscaRotinas.Read())
                        {
                            Rotina rotina = new Rotina
                            {
                                Codsubmodulo = buscaSubModulos.GetInt32(0),
                                Codrotina = buscaRotinas.GetInt32(0),
                                Descrotina = buscaRotinas.GetString(1),
                                Acaorotina = buscaRotinas.GetString(2),
                                Nomerotina = buscaRotinas.GetString(3)
                            };

                            ListaRotina.Add(rotina);
                        }

                        subModulo.Rotinas = ListaRotina;

                        ListaSubModulo.Add(subModulo);
                    }

                    modulo.Submodulo = ListaSubModulo;

                    ListaModulo.Add(modulo);
                }

                listaRotinas.ListaMenu = ListaModulo;

                return listaRotinas;
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

        public DataTable BuscaFiliais(int matricula)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable filiais = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("select f.codigo");
                query.Append("  from pcfilial f ");
                query.Append($"where f.dtexclusao is null");
                query.Append("   and f.codigo <> 99");
                query.Append($"  and f.codigo in (select codigoa from pclib where codfunc = {matricula} and codtabela = 1 )");
                query.Append(" order by case when f.codigo = 7 then '07' else f.codigo end");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(filiais);

                return filiais;
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

    public class Modulo
    {
        public int Codmodulo { get; set; }
        public string Descmodulo { get; set; }
        public List<SubModulo> Submodulo { get; set; }
    }

    public class SubModulo
    {
        public int Codmodulo { get; set; }
        public int Codsubmodulo { get; set; }
        public string Descsubmodulo { get; set; }
        public List<Rotina> Rotinas { get; set; }
    }

    public class Rotina
    {
        public int Codrotina { get; set; }
        public string Descrotina { get; set; }
        public string Acaorotina { get; set; }
        public string Nomerotina { get; set; }
        public int Codsubmodulo { get; set; }
    }
}
