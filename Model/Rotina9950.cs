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
    public class Rotina9950
    {
        public int Codfilial { get; set; }
        public string Datainicial { get; set; }
        public string Datafinal { get; set; }
        public string Rotas { get; set; }
        public int? Carga { get; set; }
        public Int64? Numped { get; set; }
        public Int64? Numtranswms { get; set; }
        public Int64? Numcar { get; set; }
        public Int64? Numos { get; set; }
        public int? Tipoos { get; set; }
        public string Modelosep { get; set; }
        public Boolean Gerados { get; set; }
        public Boolean Faturados { get; set; }
        public Boolean Impressos { get; set; }

        public DataTable BuscaRotas()
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable rotas = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append("SELECT CODROTA AS VALUE, CODROTA || ' - ' || DESCRICAO AS LABEL FROM PCROTAEXP ORDER BY DESCRICAO");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(rotas);

                return rotas;
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

        public DataTable BoxFilial(int filial)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable box = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                query.Append($"SELECT CODBOX, DESCRICAO FROM PCBOXWMS WHERE CODFILIAL = {filial} ORDER BY CODBOX");

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(box);

                return box;
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

        public DataTable BuscaDadosCargaPedidos(Rotina9950 parametros)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            try
            {
                if (parametros.Modelosep != "C")
                {
                    DataTable pedidos = new DataTable();
                    StringBuilder queryPedido = new StringBuilder();


                    queryPedido.Append("select case when numtranswms like trim(pedidos) || '%' then null else numtranswms end numtranswms, ");
                    queryPedido.Append("       pedidos, cliente, rota, praca, vlped, peso, volume, os13, vol13, os20, vol20,");
                    queryPedido.Append("       os17, vol17, separacao, conferencia, divergencia, obswms");
                    queryPedido.Append("  from (select nvl(ped.numtranswms, ped.numped) as numtranswms, ");
                    queryPedido.Append("               wm_concat(ped.numped || ' ') as pedidos, ");
                    queryPedido.Append("               cli.codcli || ' - ' || cli.cliente as cliente, ");
                    queryPedido.Append("               rot.codrota || ' - ' || rot.descricao as rota, ");
                    queryPedido.Append("               ped.codpraca || ' - ' || praca.praca as praca, ");
                    queryPedido.Append("               round(sum(ped.vlatend), 2) as vlped, ");
                    queryPedido.Append("               round(sum(ped.totpeso), 2) as peso, ");
                    queryPedido.Append("               round(sum(ped.totvolume), 3) as volume, ");
                    queryPedido.Append("               max(nvl(os_vol.os_13, 0)) as os13, ");
                    queryPedido.Append("               max(nvl(os_vol.vol_13, 0)) as vol13, ");
                    queryPedido.Append("               max(nvl(os_vol.os_20, 0)) as os20, ");
                    queryPedido.Append("               max(nvl(os_vol.vol_20, 0)) as vol20, ");
                    queryPedido.Append("               max(nvl(os_vol.os_17, 0)) as os17, ");
                    queryPedido.Append("               max(nvl(os_vol.vol_17, 0)) as vol17, ");
                    queryPedido.Append("               max(nvl(perc.separacao, 0)) as separacao, ");
                    queryPedido.Append("               max(nvl(perc.conferencia, 0)) as conferencia, ");
                    queryPedido.Append("               max(nvl(perc.divergencia, 0)) as divergencia, ");
                    queryPedido.Append("               ped.obsentrega4 as obswms");
                    queryPedido.Append("          from pcpedc ped inner join pcclient cli on (ped.codcli = cli.codcli)");
                    queryPedido.Append("                          inner join pcpraca praca on (ped.codpraca = praca.codpraca)");
                    queryPedido.Append("                          inner join pcrotaexp rot on (praca.rota = rot.codrota)");
                    if (parametros.Numos != null || parametros.Tipoos != null || parametros.Gerados || parametros.Numtranswms != null)
                    {
                        queryPedido.Append("                      inner join (select numtranswms, ");
                    }
                    else
                    {
                        queryPedido.Append("                      left outer join (select numtranswms, ");
                    }
                    queryPedido.Append("                                                  round((count(case when dtfimseparacao is not null then 1 end) / count(*)) * 100, 2) as separacao, ");
                    queryPedido.Append("                                                  round(( sum(nvl(qtconferida, 0)) * 100) / sum(qt), 2) as conferencia, ");
                    queryPedido.Append("                                                  round(((sum(qt) - sum(nvl(qtconferida, 0))) * 100) / sum(qt), 2) as divergencia");
                    queryPedido.Append("                                             from pcmovendpend");
                    queryPedido.Append("                                            where data > sysdate - 90");
                    queryPedido.Append("                                              and dtestorno is null");
                    queryPedido.Append($"                                             and codfilial = {parametros.Codfilial}");
                    queryPedido.Append("                                              and codoper = 'S'");
                    if (parametros.Numos != null)
                    {
                        queryPedido.Append($"                                         and numos = {parametros.Numos}");
                    }
                    if (parametros.Numtranswms != null)
                    {
                        queryPedido.Append($"                                         and numtranswms = {parametros.Numtranswms}");
                    }
                    if (parametros.Tipoos != null)
                    {
                        queryPedido.Append($"                                         and tipoos in ({parametros.Tipoos})");
                    }
                    else
                    {
                        queryPedido.Append("                                          and tipoos in (13, 17, 20)");
                    }
                    queryPedido.Append("                                            group by numtranswms) perc on(ped.numtranswms = perc.numtranswms)");
                    if (parametros.Numos != null || parametros.Tipoos != null || parametros.Gerados || parametros.Numtranswms != null)
                    {
                        queryPedido.Append("                      inner join      (select numtranswms, max(os_13) as os_13, max(os_17) as os_17, max(os_20) as os_20, ");
                    }
                    else
                    {
                        queryPedido.Append("                      left outer join (select numtranswms, max(os_13) as os_13, max(os_17) as os_17, max(os_20) as os_20, ");
                    }                    
                    queryPedido.Append("                                                  max(vol_13) as vol_13, max(vol_17) as vol_17, max(vol_20) as vol_20");
                    queryPedido.Append("                                             from (select mov.numtranswms,");
                    queryPedido.Append("                                                          case when mov.tipoos = 13 then count(distinct mov.numos) else 0 end os_13,");
                    queryPedido.Append("                                                          case when mov.tipoos = 17 then count(distinct mov.numos) else 0 end os_17,");
                    queryPedido.Append("                                                          case when mov.tipoos = 20 then count(distinct mov.numos) else 0 end os_20,");
                    queryPedido.Append("                                                          case when mov.tipoos = 13 then count(vol.numos) else 0 end vol_13,");
                    queryPedido.Append("                                                          case when mov.tipoos = 17 then count(vol.numos) else 0 end vol_17,");
                    queryPedido.Append("                                                          case when mov.tipoos = 20 then count(vol.numos) else 0 end vol_20");
                    queryPedido.Append("                                                     from (select distinct numtranswms, numos, tipoos from pcmovendpend ");
                    queryPedido.Append("                                                            where data > sysdate - 90");
                    queryPedido.Append("                                                              and dtestorno is null");
                    queryPedido.Append($"                                                             and codfilial = {parametros.Codfilial}");
                    queryPedido.Append("                                                              and codoper = 'S'");
                    if (parametros.Tipoos != null)
                    {
                        queryPedido.Append($"                                                         and tipoos in ({parametros.Tipoos})");
                    }
                    else
                    {
                        queryPedido.Append("                                                          and tipoos in (13, 17, 20)");
                    }
                    if (parametros.Numos != null)
                    {
                        queryPedido.Append($"                                                         and numos = {parametros.Numos}");
                    }
                    if (parametros.Numtranswms != null)
                    {
                        queryPedido.Append($"                                                         and numtranswms = {parametros.Numtranswms}");
                    }
                    queryPedido.Append("                                                          ) mov left outer join pcvolumeos vol on(mov.numos = vol.numos)");
                    queryPedido.Append("                                                    group by mov.numtranswms, mov.tipoos)");
                    queryPedido.Append("                                            group by numtranswms) os_vol on (ped.numtranswms = os_vol.numtranswms) ");
                    queryPedido.Append($"where ped.codfilial = {parametros.Codfilial}");
                    if (parametros.Numped != null)
                    {
                    queryPedido.Append($"  and ped.numped = {parametros.Numped}");
                    }
                    if (parametros.Numtranswms != null)
                    {
                    queryPedido.Append($"  and ped.numtranswms = {parametros.Numtranswms}");
                    }
                    if (parametros.Gerados)
                    {
                    queryPedido.Append("   and ped.dtwms is not null");
                    }
                    else
                    {
                    queryPedido.Append("   and ped.dtwms is null");
                    }
                    if (parametros.Faturados)
                    {
                        queryPedido.Append("   and ped.posicao = 'F'");
                    }
                    else
                    {
                        queryPedido.Append("   and ped.posicao = 'L'");
                    }
                    if (parametros.Gerados)
                    {
                    queryPedido.Append($"  and trunc(ped.dtwms) between to_date('{parametros.Datainicial}', 'dd-mm-yyyy') and to_date('{parametros.Datafinal}', 'dd-mm-yyyy')");
                    }
                    else
                    {
                    queryPedido.Append($"  and ped.dtentrega between to_date('{parametros.Datainicial}', 'dd-mm-yyyy') and to_date('{parametros.Datafinal}', 'dd-mm-yyyy')");
                    }
                    if (!string.IsNullOrEmpty(parametros.Rotas))
                    {
                    queryPedido.Append($"  and rot.codrota in ({parametros.Rotas})");
                    }
                    queryPedido.Append(" group by nvl(ped.numtranswms, ped.numped),");
                    queryPedido.Append("          cli.codcli || ' - ' || cli.cliente, ");
                    queryPedido.Append("          rot.codrota || ' - ' || rot.descricao, ");
                    queryPedido.Append("          ped.codpraca || ' - ' || praca.praca, ");
                    queryPedido.Append("          ped.obsentrega4) ");
                    if (parametros.Gerados && parametros.Modelosep == "A")
                    {
                        if (!parametros.Impressos)
                        {
                        queryPedido.Append(" where os13 > 0 and vol13 = 0");
                        }
                        else
                        {
                        queryPedido.Append(" where os13 > 0 and vol13 > 0");
                        }                      
                    }
                    if (parametros.Gerados && parametros.Modelosep == "P")
                    {
                        if (!parametros.Impressos)
                        {
                            queryPedido.Append(" where not ((case when os13 <= vol13 then 'S' else 'N' end = 'S') and ");
                            queryPedido.Append("            (case when os17 <= vol17 then 'S' else 'N' end = 'S') and ");
                            queryPedido.Append("            (case when os20 <= vol20 then 'S' else 'N' end = 'S')) ");
                        }
                        else
                        {
                            queryPedido.Append(" where ((case when os13 <= vol13 then 'S' else 'N' end = 'S') and ");
                            queryPedido.Append("        (case when os17 <= vol17 then 'S' else 'N' end = 'S') and ");
                            queryPedido.Append("        (case when os20 <= vol20 then 'S' else 'N' end = 'S')) ");
                        }
                    }
                    queryPedido.Append(" order by rota, praca, cliente ");

                    exec.CommandText = queryPedido.ToString();
                    OracleDataAdapter oda = new OracleDataAdapter(exec);
                    oda.SelectCommand = exec;
                    oda.Fill(pedidos);
                    
                    return pedidos;
                } 
                else
                {
                    DataTable carga = new DataTable();
                    StringBuilder queryCarga = new StringBuilder();

                    queryCarga.Append("select numcar, ");
                    queryCarga.Append("       destino, ");
                    queryCarga.Append("       numbox, ");
                    queryCarga.Append("       vltotal, ");
                    queryCarga.Append("       peso, ");
                    queryCarga.Append("       volume, ");
                    queryCarga.Append("       os13, ");
                    queryCarga.Append("       vol13, ");
                    queryCarga.Append("       os20, ");
                    queryCarga.Append("       vol20, ");
                    queryCarga.Append("       os17, ");
                    queryCarga.Append("       vol17, ");
                    queryCarga.Append("       separacao, ");
                    queryCarga.Append("       conferencia, ");
                    queryCarga.Append("       divergencia, ");
                    queryCarga.Append("       obswms");
                    queryCarga.Append("  from (select car.numcar, ");
                    queryCarga.Append("               car.destino, ");
                    queryCarga.Append("               os_vol.numbox, ");
                    queryCarga.Append("               round(sum(car.vltotal), 2) as vltotal, ");
                    queryCarga.Append("               round(sum(car.totpeso), 2) as peso, ");
                    queryCarga.Append("               round(sum(car.totvolume), 3) as volume, ");
                    queryCarga.Append("               max(nvl(os_vol.os_13, 0)) as os13, ");
                    queryCarga.Append("               max(nvl(os_vol.vol_13, 0)) as vol13, ");
                    queryCarga.Append("               max(nvl(os_vol.os_20, 0)) as os20, ");
                    queryCarga.Append("               max(nvl(os_vol.vol_20, 0)) as vol20, ");
                    queryCarga.Append("               max(nvl(os_vol.os_17, 0)) as os17, ");
                    queryCarga.Append("               max(nvl(os_vol.vol_17, 0)) as vol17, ");
                    queryCarga.Append("               max(nvl(perc.separacao, 0)) as separacao, ");
                    queryCarga.Append("               max(nvl(perc.conferencia, 0)) as conferencia, ");
                    queryCarga.Append("               max(nvl(perc.divergencia, 0)) as divergencia, ");
                    queryCarga.Append("               car.obsexportacao as obswms");
                    if (parametros.Numos != null || parametros.Tipoos != null || parametros.Gerados)
                    {
                        queryCarga.Append("      from pccarreg car inner join      (select numcar, ");
                    }
                    else
                    {
                        queryCarga.Append("      from pccarreg car left outer join (select numcar, ");
                    }
                    queryCarga.Append("                                                    round((count(case when dtfimseparacao is not null then 1 end * 100) / count(*)) * 100, 2) as separacao, ");
                    queryCarga.Append("                                                    round(( sum(nvl(qtconferida, 0)) * 100) / sum(qt), 2) as conferencia, ");
                    queryCarga.Append("                                                    round(((sum(qt) - sum(nvl(qtconferida, 0))) * 100) / sum(qt), 2) as divergencia");
                    queryCarga.Append("                                               from pcmovendpend");
                    queryCarga.Append("                                              where data > sysdate - 90");
                    queryCarga.Append("                                                and dtestorno is null");
                    if (parametros.Numos != null)
                    {
                        queryCarga.Append($"                                           and numos = {parametros.Numos}");
                    }
                    if (parametros.Tipoos != null)
                    {
                        queryCarga.Append($"                                           and tipoos in ({parametros.Tipoos})");
                    }
                    else
                    {
                        queryCarga.Append("                                            and tipoos in (13, 17, 20)");
                    }
                    if (parametros.Numcar != null)
                    {
                        queryCarga.Append($"                                           and numcar = {parametros.Numcar}");
                    }
                    queryCarga.Append($"                                               and codfilial = {parametros.Codfilial}");
                    queryCarga.Append("                                                and codoper = 'S'");
                    queryCarga.Append("                                              group by numcar) perc on (car.numcar = perc.numcar)");
                    if (parametros.Numos != null || parametros.Tipoos != null || parametros.Gerados)
                    {
                        queryCarga.Append("                        inner join      (select numcar, numbox, max(os_13) as os_13, max(os_17) as os_17, max(os_20) as os_20, ");
                    }
                    else
                    {
                        queryCarga.Append("                        left outer join (select numcar, numbox, max(os_13) as os_13, max(os_17) as os_17, max(os_20) as os_20, "); 
                    }
                    queryCarga.Append("                                                    max(vol_13) as vol_13, max(vol_17) as vol_17, max(vol_20) as vol_20");
                    queryCarga.Append("                                               from (select mov.numcar, max(mov.numbox) as numbox, ");
                    queryCarga.Append("                                                            case when mov.tipoos = 13 then count(distinct mov.numos) else 0 end os_13,");
                    queryCarga.Append("                                                            case when mov.tipoos = 17 then count(distinct mov.numos) else 0 end os_17,");
                    queryCarga.Append("                                                            case when mov.tipoos = 20 then count(distinct mov.numos) else 0 end os_20,");
                    queryCarga.Append("                                                            case when mov.tipoos = 13 then count(vol.numos) else 0 end vol_13,");
                    queryCarga.Append("                                                            case when mov.tipoos = 17 then count(vol.numos) else 0 end vol_17,");
                    queryCarga.Append("                                                            case when mov.tipoos = 20 then count(vol.numos) else 0 end vol_20");
                    queryCarga.Append("                                                       from (select numcar, numos, tipoos, max(nvl(codbox, numbox))  OVER (PARTITION BY numcar) as numbox");
                    queryCarga.Append("                                                               from pcmovendpend mov");
                    queryCarga.Append("                                                              where mov.data > sysdate - 90");
                    queryCarga.Append("                                                                and mov.dtestorno is null");
                    if (parametros.Numos != null)
                    {
                        queryCarga.Append($"                                                           and mov.numos = {parametros.Numos}");
                    }
                    if (parametros.Tipoos != null)
                    {
                        queryCarga.Append($"                                                           and mov.tipoos in ({parametros.Tipoos})");
                    }
                    else
                    {
                        queryCarga.Append("                                                            and mov.tipoos in (13, 17, 20)");
                    }
                    if (parametros.Numcar != null)
                    {
                        queryCarga.Append($"                                                           and mov.numcar = {parametros.Numcar}");
                    }
                    queryCarga.Append($"                                                               and mov.codfilial = {parametros.Codfilial}");
                    queryCarga.Append("                                                                and mov.codoper = 'S'");
                    queryCarga.Append("                                                          group by numcar, numos, tipoos, nvl(codbox, numbox)) mov left outer join pcvolumeos vol on (mov.numos = vol.numos)");
                    queryCarga.Append("                                                      group by mov.numcar, mov.tipoos)");
                    queryCarga.Append("                                              group by numcar, numbox) os_vol on (car.numcar = os_vol.numcar)");
                    queryCarga.Append($"        where car.datamon between to_date('{parametros.Datainicial}', 'dd-mm-yyyy') and to_date('{parametros.Datafinal}', 'dd-mm-yyyy') ");
                    if (parametros.Numcar != null)
                    {
                        queryCarga.Append($"      and car.numcar = {parametros.Numcar}");
                    }
                    if (!string.IsNullOrEmpty(parametros.Rotas))
                    {
                        queryCarga.Append("       and exists (select 1");
                        queryCarga.Append("                    from pcpedc pc inner join pcpraca pr on (pc.codpraca = pr.codpraca)");
                        queryCarga.Append("                                   inner join pcrotaexp rot on (pr.rota = rot.codrota)");
                        queryCarga.Append("                   where pc.numcar = car.numcar");
                        queryCarga.Append($"                    and rot.codrota in ({parametros.Rotas}))");
                    }
                    if (parametros.Gerados)
                    {
                        queryCarga.Append("       and exists (select numcar, sum(tot_pedidos) as tot_pedidos, sum(nvl(tot_ped_wms, 0)) as tot_ped_wms");
                        queryCarga.Append("                     from (select numcar,");
                        queryCarga.Append("                                  count(numped) as tot_pedidos,");
                        queryCarga.Append("                                  case when dtwms is not null then count(numped) end tot_ped_wms");
                        queryCarga.Append("                             from pcpedc pc");
                        queryCarga.Append("                            where numcar = car.numcar");
                        queryCarga.Append("                            group by numcar, dtwms)");
                        queryCarga.Append("                    group by numcar");
                        queryCarga.Append("                   having sum(tot_pedidos) = sum(nvl(tot_ped_wms, 0)))");
                    }
                    else
                    {
                        queryCarga.Append("       and exists (select numcar, sum(tot_pedidos) as tot_pedidos, sum(nvl(tot_ped_wms, 0)) as tot_ped_wms");
                        queryCarga.Append("                     from (select numcar,");
                        queryCarga.Append("                                  count(numped) as tot_pedidos,");
                        queryCarga.Append("                                  case when dtwms is not null then count(numped) end tot_ped_wms");
                        queryCarga.Append("                             from pcpedc pc");
                        queryCarga.Append("                            where numcar = car.numcar");
                        queryCarga.Append("                            group by numcar, dtwms)");
                        queryCarga.Append("                    group by numcar");
                        queryCarga.Append("                   having (((sum(tot_pedidos) <> sum(nvl(tot_ped_wms, 0))) or (sum(nvl(tot_ped_wms, 0)) = 0)) or not exists (select 1 from pcmovendpend where numcar = car.numcar)))");
                    }                    
                    queryCarga.Append("           and exists (select 1");
                    queryCarga.Append("                         from pcpedc");
                    queryCarga.Append("                        where numcar = car.numcar");
                    if (parametros.Faturados)
                    {
                        queryCarga.Append("                      and posicao = 'F'");
                    }
                    else
                    {
                        queryCarga.Append("                      and posicao = 'M'");
                    }
                    queryCarga.Append($"                         and codfilial = {parametros.Codfilial})");
                    queryCarga.Append("         group by car.numcar,");
                    queryCarga.Append("                  car.destino,");
                    queryCarga.Append("                  os_vol.numbox,");
                    queryCarga.Append("                  car.obsexportacao)");
                    if (parametros.Gerados)
                    {
                        if (!parametros.Impressos)
                        {
                            queryCarga.Append(" where not ((case when os13 <= vol13 then 'S' else 'N' end = 'S') and ");
                            queryCarga.Append("            (case when os17 <= vol17 then 'S' else 'N' end = 'S') and ");
                            queryCarga.Append("            (case when os20 <= vol20 then 'S' else 'N' end = 'S')) ");
                        }
                        else
                        {
                            queryCarga.Append(" where ((case when os13 <= vol13 then 'S' else 'N' end = 'S') and ");
                            queryCarga.Append("        (case when os17 <= vol17 then 'S' else 'N' end = 'S') and ");
                            queryCarga.Append("        (case when os20 <= vol20 then 'S' else 'N' end = 'S')) ");
                        }
                    }
                    queryCarga.Append("order by numcar");

                    exec.CommandText = queryCarga.ToString();
                    OracleDataAdapter oda = new OracleDataAdapter(exec);
                    oda.SelectCommand = exec;
                    oda.Fill(carga);

                    return carga;
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

        public Boolean LiberarCarregamento(int numcar)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();
            StringBuilder finaliza = new StringBuilder();

            try
            {
                finaliza.Append($"UPDATE PCMOVENDPEND SET POSICAO = 'A', DTINICIOOS = NVL(DTINICIOOS,SYSDATE), DTFIMSEPARACAO = NVL(DTFIMSEPARACAO, SYSDATE), ");
                finaliza.Append($"                        CODFUNCOS = NVL(CODFUNCOS, 1), CODFUNCOSFIM = 1, DTFIMOSFILA = SYSDATE");
                finaliza.Append($" WHERE NUMCAR = {numcar} AND POSICAO = 'P' AND DTESTORNO IS NULL");

                exec.CommandText = finaliza.ToString();
                OracleDataReader reader = exec.ExecuteReader();

                return true;
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

    public class Rotina9950Gerar
    {
        public Int64? Numped { get; set; }
        public Int32? Numcar { get; set; }
        public int Numbox { get; set; }
        public string Modelosep { get; set; }
        public int Codfuncger { get; set; }
        public Boolean Homologacao { get; set; }

        public string GerarOs(List<Rotina9950Gerar> dados)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleTransaction transacao = connection.BeginTransaction();
            OracleCommand updatePedCar = connection.CreateCommand();
            OracleCommand abastecimento = connection.CreateCommand();
            OracleCommand exec = new OracleCommand("stp_gera_os_wms", connection);

            updatePedCar.Transaction = transacao;

            string respostaGeracao = "";
            string cargasComAntecipPendente = "";

            try
            {
                dados.ForEach(dado =>
                {
                    StringBuilder marcaPedidosCarga = new StringBuilder();

                    if (dado.Modelosep == "C")
                    {

                        marcaPedidosCarga.Append($"UPDATE PCPEDC SET USAINTEGRACAOWMS = 'S', CODFUNCEXPINTWMS = {dado.Codfuncger}, DTIMPORTACAOWMS = SYSDATE WHERE NUMCAR = {dado.Numcar}");

                        updatePedCar.CommandText = marcaPedidosCarga.ToString();
                        OracleDataReader updateCarga = updatePedCar.ExecuteReader();

                        if (dado.Homologacao)
                        {
                            exec = new OracleCommand("stp_gera_os_wms_homologacao", connection);
                        }
                        else
                        {
                            exec = new OracleCommand("stp_gera_os_wms", connection);
                        }

                        exec.CommandType = CommandType.StoredProcedure;

                        exec.Parameters.Add("p_codfuncger", OracleDbType.Int32).Value = dado.Codfuncger;
                        exec.Parameters.Add("p_codbox", OracleDbType.Int32).Value = dado.Numbox;
                        exec.Parameters.Add("p_codrotina", OracleDbType.Int32).Value = 9950; // ROTINA QUE ESTÁ GERANDO
                        exec.Parameters.Add("p_antecipado", OracleDbType.Varchar2).Value = "N"; // ANTECIPADO S OU N
                        exec.Parameters.Add("p_tolerancia", OracleDbType.Int32).Value = 0; // TOLERÂNCIA

                        OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 1000)
                        {
                            Direction = ParameterDirection.Output
                        };

                        exec.Parameters.Add(resposta);

                        exec.ExecuteNonQuery();

                        StringBuilder validaAntecipadoSemConf = new StringBuilder();

                        validaAntecipadoSemConf.Append("SELECT NUMCAR");
                        validaAntecipadoSemConf.Append("  FROM PCMOVENDPEND ");
                        validaAntecipadoSemConf.Append($"WHERE NUMCAR = {dado.Numcar} AND SEPARACAOANTECIPADA = 'S' ");
                        validaAntecipadoSemConf.Append("   AND NVL(QT, 0) <> NVL(QTCONFERIDA, 0) AND DTESTORNO IS NULL ");
                        validaAntecipadoSemConf.Append(" GROUP BY NUMCAR");

                        updatePedCar.CommandText = validaAntecipadoSemConf.ToString();
                        OracleDataReader validacaoAntecipado = updatePedCar.ExecuteReader();

                        if (validacaoAntecipado.Read())
                        {
                            int carga = validacaoAntecipado.GetInt32(0);

                            cargasComAntecipPendente += carga + " ";
                        }

                        respostaGeracao = resposta.Value.ToString();
                    }
                    else
                    {
                        marcaPedidosCarga.Append($"UPDATE PCPEDC SET USAINTEGRACAOWMS = 'S', CODFUNCEXPINTWMS = {dado.Codfuncger}, DTIMPORTACAOWMS = SYSDATE WHERE NUMPED = {dado.Numped}");

                        updatePedCar.CommandText = marcaPedidosCarga.ToString();
                        OracleDataReader updatePedido = updatePedCar.ExecuteReader();
                    }
                });


                if (dados[0].Modelosep != "C")
                {
                    if (dados[0].Homologacao)
                    {
                        exec = new OracleCommand("stp_gera_os_wms_homologacao", connection);
                    }
                    else
                    {
                        exec = new OracleCommand("stp_gera_os_wms", connection);
                    }

                    exec.CommandType = CommandType.StoredProcedure;

                    exec.Parameters.Add("p_codfuncger", OracleDbType.Int32).Value = dados[0].Codfuncger;
                    exec.Parameters.Add("p_codbox", OracleDbType.Int32).Value = dados[0].Numbox;
                    exec.Parameters.Add("p_codrotina", OracleDbType.Int32).Value = 9950; // ROTINA QUE ESTÁ GERANDO
                    exec.Parameters.Add("p_antecipado", OracleDbType.Varchar2).Value = "S"; // ROTINA QUE ESTÁ GERANDO
                    exec.Parameters.Add("p_tolerancia", OracleDbType.Int32).Value = 0; // TOLERÂNCIA

                    OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 1000)
                    {
                        Direction = ParameterDirection.Output
                    };

                    exec.Parameters.Add(resposta);

                    exec.ExecuteNonQuery();

                    transacao.Commit();

                    return resposta.Value.ToString();
                }
                else
                {
                    StringBuilder verificaJob = new StringBuilder();
                    StringBuilder geraAbastecimentoCarga = new StringBuilder();

                    if (dados[0].Homologacao)
                    {
                        verificaJob.Append("SELECT CASE WHEN COUNT(*) > 0 THEN 'S' ELSE 'N' END AS v_job_running FROM SYS.ALL_SCHEDULER_RUNNING_JOBS WHERE UPPER(JOB_NAME) = 'GERA_ABASTECIMENTO_CORRETIVO_HOMOLOGACAO'");
                    }
                    else
                    {
                        verificaJob.Append("SELECT CASE WHEN COUNT(*) > 0 THEN 'S' ELSE 'N' END AS v_job_running FROM SYS.ALL_SCHEDULER_RUNNING_JOBS WHERE UPPER(JOB_NAME) = 'GERA_ABASTECIMENTO_CORRETIVO'");
                    }

                    abastecimento.CommandText = verificaJob.ToString();
                    OracleDataReader jobExecucao = abastecimento.ExecuteReader();

                    if (jobExecucao.Read())
                    {
                        string emExecucao = jobExecucao.GetString(0);

                        if (emExecucao == "N")
                        {
                            if (dados[0].Homologacao)
                            {
                                geraAbastecimentoCarga.Append("BEGIN DBMS_SCHEDULER.RUN_JOB(JOB_NAME => 'GERA_ABASTECIMENTO_CORRETIVO_HOMOLOGACAO', USE_CURRENT_SESSION => FALSE); END;");
                            }
                            else
                            {
                                geraAbastecimentoCarga.Append("BEGIN DBMS_SCHEDULER.RUN_JOB(JOB_NAME => 'GERA_ABASTECIMENTO_CORRETIVO', USE_CURRENT_SESSION => FALSE); END;");
                            }

                            abastecimento.CommandText = geraAbastecimentoCarga.ToString();
                            OracleDataReader execAbastecimento = abastecimento.ExecuteReader();
                        }
                    }

                    transacao.Commit();

                    if (!string.IsNullOrEmpty(cargasComAntecipPendente))
                    {
                        return respostaGeracao + " Carga(s) com antecipado pendente: " + cargasComAntecipPendente;
                    } 
                    else
                    {
                        return respostaGeracao;
                    }

                    return respostaGeracao;

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
                updatePedCar.Dispose();
                abastecimento.Dispose();
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
                updatePedCar.Dispose();
                abastecimento.Dispose();
                exec.Dispose();
                connection.Dispose();
            }
        }
    }

    public class Rotina9950Impressao
    {
        public string Antecipado { get; set; }
        public string Modelo { get; set; }
        public string CargasTransacoes { get; set; }
        public int Tipoos { get; set; }
        public int Codfunc { get; set; }

        public Boolean GerarVolumesWMS(Rotina9950Impressao dados)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();
            StringBuilder geraVolume = new StringBuilder();

            geraVolume.Append(" CALL PRC_GERA_VOLUME_WMS_NUMTRANS(V_CODFUNC => " + dados.Codfunc + " , V_CODROTINA => 9950 ");

            if (dados.Modelo == "C")
            {
                geraVolume.Append(" , P_NUMCAR => '" + dados.CargasTransacoes + "'");
            }
            else
            {
                geraVolume.Append(" , P_NUMTRANSWMS => '" + dados.CargasTransacoes + "'");
            }

            if (dados.Tipoos == 17)
            {
                geraVolume.Append(" , P_TIPO_17 => 'S' ");
            }
            else if (dados.Tipoos == 20)
            {
                geraVolume.Append(" , P_TIPO_20 => 'S' ");
            }
            if (dados.Tipoos == 13)
            {
                geraVolume.Append(" , P_TIPO_13 => 'S' ");
            }

            geraVolume.Append(" ) ");
            exec.CommandText = geraVolume.ToString();

            exec.ExecuteNonQuery();

            connection.Close();

            return true;
        }
    }

    public class Rotina9950Estornar
    {
        public Int64? Numtranswms { get; set; }
        public Int32? Numcar { get; set; }
        public string Modelosep { get; set; }
        public int Codfuncger { get; set; }
        public Boolean Homologacao { get; set; }

        public string EstornarOs(List<Rotina9950Estornar> dados)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = new OracleCommand("stp_estorna_wms", connection);

            string retornoProcedure = "Estorno realizado com sucesso!";

            try
            {
                dados.ForEach(dado =>
                {
                    if (dado.Homologacao)
                    {
                        exec = new OracleCommand("stp_estorna_wms_homologacao", connection)
                        {
                            CommandType = CommandType.StoredProcedure
                        };
                    } 
                    else
                    {
                        exec = new OracleCommand("stp_estorna_wms", connection)
                        {
                            CommandType = CommandType.StoredProcedure
                        };
                    }
                    

                    exec.Parameters.Add("p_codfuncger", OracleDbType.Varchar2).Value = dado.Codfuncger;
                    if (dado.Modelosep == "C")
                    {
                        exec.Parameters.Add("p_numtranswms", OracleDbType.Int32).Value = -1;
                    }
                    else
                    {
                        exec.Parameters.Add("p_numtranswms", OracleDbType.Int32).Value = dado.Numtranswms;
                    }
                    if (dado.Modelosep == "C")
                    {
                        exec.Parameters.Add("p_numCar", OracleDbType.Int32).Value = dado.Numcar;
                    }
                    else
                    {
                        exec.Parameters.Add("p_numCar", OracleDbType.Int32).Value = -1;
                    }

                    OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 10000)
                    {
                        Direction = ParameterDirection.Output
                    };

                    exec.Parameters.Add(resposta);

                    exec.ExecuteNonQuery();

                    if (resposta.Value.ToString() != "Estorno realizado com sucesso!")
                    {
                        retornoProcedure = resposta.Value.ToString();
                    }
                });
                
                return retornoProcedure;
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

        public DataTable BuscaEstorno(ParamEstorno dados)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable estorno = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {
                if (dados.TipoPesquisa == "C")
                {
                    query.Append("select mov.codfilial, mov.numcar, nvl(mov.numpalete, 0) numpalete, mov.numtranswms, mov.numbox, mov.numos, ");
                    query.Append("       letras(pc.codcli) || ' - ' || cli.codcli || ' - ' || cli.cliente as cliente, mov.tipoos, mov.codprod || ' - ' || prod.descricao as produto");
                    query.Append("  from pcmovendpend mov inner join pcprodut prod on (mov.codprod = prod.codprod)");
                    query.Append("                        inner join pcpedc pc on (mov.numtranswms = pc.numtranswms)");
                    query.Append("                        inner join pcclient cli on (pc.codcli = cli.codcli) ");
                    query.Append($"where mov.numcar = {dados.ValorPesquisa}");
                    query.Append("   and mov.dtestorno is null");
                    query.Append(" order by mov.numcar, mov.numpalete, mov.numtranswms, mov.tipoos");
                }
                else
                {
                    query.Append("select mov.codfilial, mov.numcar, nvl(mov.numpalete, 0) numpalete, mov.numtranswms, mov.numbox, mov.numos, ");
                    query.Append("       letras(pc.codcli) || ' - ' || cli.codcli || ' - ' || cli.cliente as cliente, mov.tipoos, mov.codprod || ' - ' || prod.descricao as produto");
                    query.Append("  from pcmovendpend mov inner join pcprodut prod on (mov.codprod = prod.codprod)");
                    query.Append("                        inner join pcpedc pc on (mov.numtranswms = pc.numtranswms)");
                    query.Append("                        inner join pcclient cli on (pc.codcli = cli.codcli) ");
                    query.Append($"where mov.numtranswms in ({dados.ValorPesquisa})");
                    query.Append("   and mov.dtestorno is null");
                    query.Append(" order by mov.numcar, mov.numpalete, mov.numtranswms, mov.tipoos");
                }                

                exec.CommandText = query.ToString();
                OracleDataAdapter oda = new OracleDataAdapter(exec);
                oda.SelectCommand = exec;
                oda.Fill(estorno);

                return estorno;
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

    public class ParamEstorno
    {
        public string TipoPesquisa { get; set; }
        public string ValorPesquisa { get; set; }
    }

    public class Rotina9950Corte
    {
        public Int32? Numcar { get; set; }
        public Int32? Numtranswms { get; set; }
        public int Codfuncger { get; set; }
        public string ModeloSep { get; set; }
        public Boolean Homologacao { get; set; }

        public string Corte(Rotina9950Corte dados)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = new OracleCommand();

            try
            {
                if (dados.Homologacao)
                {
                    exec = new OracleCommand("stp_corte_wms_homologacao", connection);
                } 
                else
                {
                    exec = new OracleCommand("stp_corte_wms", connection);
                }

                if (dados.Numcar != -1)
                {
                    exec.CommandType = CommandType.StoredProcedure;

                    exec.Parameters.Add("P_NUMCAR", OracleDbType.Int64).Value = dados.Numcar;
                    
                    exec.Parameters.Add("P_NUMTRANSWMS", OracleDbType.Int64).Value = dados.Numtranswms;

                    exec.Parameters.Add("P_FUNCCORTE", OracleDbType.Int64).Value = dados.Codfuncger;

                    exec.Parameters.Add("P_MOTIVOCORTE", OracleDbType.Int32).Value = 38; // MOTIVO DO CORTE

                    exec.Parameters.Add("P_MODELO_SEP", OracleDbType.Varchar2).Value = dados.ModeloSep;

                    OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 20000)
                    {
                        Direction = ParameterDirection.Output
                    };

                    exec.Parameters.Add(resposta);

                    exec.ExecuteNonQuery();

                    return resposta.Value.ToString();
                } 
                else
                {
                    exec.CommandType = CommandType.StoredProcedure;

                    exec.Parameters.Add("P_NUMCAR", OracleDbType.Int64).Value = dados.Numcar;
                    
                    exec.Parameters.Add("P_NUMTRANSWMS", OracleDbType.Int64).Value = dados.Numtranswms;

                    exec.Parameters.Add("P_FUNCCORTE", OracleDbType.Int64).Value = dados.Codfuncger;

                    exec.Parameters.Add("P_MOTIVOCORTE", OracleDbType.Int32).Value = 38; // MOTIVO DO CORTE

                    exec.Parameters.Add("P_MODELO_SEP", OracleDbType.Varchar2).Value = dados.ModeloSep;

                    OracleParameter resposta = new OracleParameter("@Resposta", OracleDbType.Varchar2, 20000)
                    {
                        Direction = ParameterDirection.Output
                    };

                    exec.Parameters.Add(resposta);

                    exec.ExecuteNonQuery();

                    return resposta.Value.ToString();
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

        public DataTable BuscaCortes(int valorBusca, string tipoBusca)
        {
            OracleConnection connection = DataBase.NovaConexao();
            OracleCommand exec = connection.CreateCommand();

            DataTable cortesCarga = new DataTable();
            DataTable cortesTransacao = new DataTable();

            StringBuilder query = new StringBuilder();

            try
            {   
                if (tipoBusca == "C")
                {
                    query.Append("SELECT mov.codfilial, mov.numtranswms, mov.numos, mov.tipoos, mov.numpalete, ");
                    query.Append("       mov.codprod || ' - ' || prod.descricao as produto, mov.qt AS qtos, nvl(mov.qtconferida, 0) as qtconf, ");
                    query.Append("       mov.qt - NVL(mov.qtconferida, 0) AS qtcorte, (mov.qt - (mov.qt - NVL(mov.qtconferida, 0))) as qtfaturar, ((mov.qt - NVL(mov.qtconferida, 0)) * pi.pvenda) as vlcorte,");
                    query.Append("       sum(((mov.qt - NVL(mov.qtconferida, 0)) * pi.pvenda)) over(partition by mov.numcar) as vlcortetotal");
                    query.Append("  FROM pcmovendpend mov inner join pcprodut prod on (mov.codprod = prod.codprod)");
                    query.Append("                        inner join pcpedc pc on (mov.numtranswms = pc.numtranswms and mov.numcar = pc.numcar)");
                    query.Append("                        inner join pcpedi pi on (pc.numped = pi.numped and mov.codprod = pi.codprod) ");
                    query.Append($"WHERE mov.numcar = {valorBusca}");
                    query.Append("   AND (mov.qt - NVL(mov.qtconferida, 0)) > 0");
                    query.Append("   AND mov.dtestorno IS NULL");
                    query.Append("   AND mov.dtfimconferencia IS NULL");
                    query.Append("   AND mov.posicao = 'P'");
                    query.Append(" ORDER BY mov.qt - NVL(mov.qtconferida, 0) DESC ");

                    exec.CommandText = query.ToString();
                    OracleDataAdapter oda = new OracleDataAdapter(exec);
                    oda.SelectCommand = exec;
                    oda.Fill(cortesCarga);

                    return cortesCarga;
                }
                else
                {
                    if(tipoBusca == "A")
                    {
                        query.Append("SELECT mov.codfilial, mov.numtranswms, mov.numos, mov.tipoos, mov.numpalete, ");
                        query.Append("       mov.codprod || ' - ' || prod.descricao as produto, mov.qt AS qtos, nvl(mov.qtconferida, 0) as qtconf, ");
                        query.Append("       mov.qt - NVL(mov.qtconferida, 0) AS qtcorte, (mov.qt - (mov.qt - NVL(mov.qtconferida, 0))) as qtfaturar, ((mov.qt - NVL(mov.qtconferida, 0)) * pi.pvenda) as vlcorte,");
                        query.Append("       sum(((mov.qt - NVL(mov.qtconferida, 0)) * pi.pvenda)) over(partition by mov.numcar) as vlcortetotal");
                        query.Append("  FROM pcmovendpend mov inner join pcprodut prod on (mov.codprod = prod.codprod)");
                        query.Append("                        inner join pcpedc pc on (mov.numtranswms = pc.numtranswms and mov.numcar = pc.numcar)");
                        query.Append("                        inner join pcpedi pi on (pc.numped = pi.numped and mov.codprod = pi.codprod) ");
                        query.Append($"WHERE mov.numtranswms = {valorBusca}");
                        query.Append("   AND (mov.qt - NVL(mov.qtconferida, 0)) > 0");
                        query.Append("   AND mov.dtestorno IS NULL");
                        query.Append("   AND mov.dtfimconferencia IS NULL");
                        query.Append("   AND mov.separacaoantecipada = 'S'");
                        query.Append("   AND mov.posicao = 'P'");
                        query.Append("   AND mov.tipoos = 13");
                        query.Append(" ORDER BY mov.qt - NVL(mov.qtconferida, 0) DESC ");

                        exec.CommandText = query.ToString();
                        OracleDataAdapter oda = new OracleDataAdapter(exec);
                        oda.SelectCommand = exec;
                        oda.Fill(cortesTransacao);

                        return cortesTransacao;
                    }
                    else
                    {
                        query.Append("SELECT mov.codfilial, mov.numtranswms, mov.numos, mov.tipoos, mov.numpalete, ");
                        query.Append("       mov.codprod || ' - ' || prod.descricao as produto, mov.qt AS qtos, nvl(mov.qtconferida, 0) as qtconf, ");
                        query.Append("       mov.qt - NVL(mov.qtconferida, 0) AS qtcorte, (mov.qt - (mov.qt - NVL(mov.qtconferida, 0))) as qtfaturar, ((mov.qt - NVL(mov.qtconferida, 0)) * pi.pvenda) as vlcorte,");
                        query.Append("       sum(((mov.qt - NVL(mov.qtconferida, 0)) * pi.pvenda)) over(partition by mov.numcar) as vlcortetotal");
                        query.Append("  FROM pcmovendpend mov inner join pcprodut prod on (mov.codprod = prod.codprod)");
                        query.Append("                        inner join pcpedc pc on (mov.numtranswms = pc.numtranswms and mov.numcar = pc.numcar)");
                        query.Append("                        inner join pcpedi pi on (pc.numped = pi.numped and mov.codprod = pi.codprod) ");
                        query.Append($"WHERE mov.numtranswms = {valorBusca}");
                        query.Append("   AND (mov.qt - NVL(mov.qtconferida, 0)) > 0");
                        query.Append("   AND mov.dtestorno IS NULL");
                        query.Append("   AND mov.dtfimconferencia IS NULL");
                        query.Append("   AND mov.posicao = 'P'");
                        query.Append(" ORDER BY mov.qt - NVL(mov.qtconferida, 0) DESC ");

                        exec.CommandText = query.ToString();
                        OracleDataAdapter oda = new OracleDataAdapter(exec);
                        oda.SelectCommand = exec;
                        oda.Fill(cortesTransacao);

                        return cortesTransacao;
                    }
                    
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
    }
}
