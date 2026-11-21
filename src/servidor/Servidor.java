package servidor;

import comum.Comando;
import comum.Datagrama;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Servidor central do sistema de arquivos via UDP.
 * Concorrência: uma thread por cliente, criada quando o comando REGISTER é
 * recebido. A responsabilidade de transferência fica na ClienteHandler e o
 * Servidor se mantém livre para aceitar novos clientes.
 * Despacho: o Servidor é o único leitor do socket UDP (por meio de ProtocoloUDP).
 * Cada datagrama é entregue à ClienteHandler do endereço de origem. Um UPLOAD ou
 * DOWNLOAD vindo de um endereço sem registro (o socket dedicado de uma transferência
 * do cliente) ganha uma ClienteHandler própria, que atende essa operação e termina.
 * Notificação em tempo real: a cada alteração na lista de arquivos o servidor
 * envia UPDATE_LIST para todos os clientes ativos.
 */
public class Servidor {

    public static final int PORTA = 5000;

    /** Intervalo de cada espera do loop principal; permite verificar o encerramento. */
    private static final int ESPERA_DO_LOOP_MS = 1000;

    /** Contador atômico para gerar ids de transferência únicos (int). */
    private final AtomicInteger CONTADOR_TRANSFERENCIAS = new AtomicInteger(1);

    private final ProtocoloUDP PROTOCOLO;

    private final GerenciadorArquivos GERENCIADOR_DE_ARQUIVOS;

    private final LogServidor LOG;

    /** Clientes registrados (REGISTER): recebem UPDATE_LIST. */
    private final Map<InetSocketAddress, ClienteHandler> CLIENTES_ATIVOS = new ConcurrentHashMap<>();

    /** Transferências em andamento, iniciadas em sockets dedicados dos clientes. */
    private final Map<InetSocketAddress, ClienteHandler> TRANSFERENCIAS_ATIVAS = new ConcurrentHashMap<>();

    private volatile boolean executando = true;



    public Servidor () throws IOException {

        this(PORTA);
    }



    public Servidor (int porta) throws IOException {

        PROTOCOLO = new ProtocoloUDP(porta);

        GERENCIADOR_DE_ARQUIVOS = new GerenciadorArquivos();

        LOG = new LogServidor();
    }



    public static void main (String[] args) {

        int porta = PORTA;

        if (args.length > 0) {

            try { porta = Integer.parseInt(args[0]); }

            catch (NumberFormatException e) { porta = PORTA; }
        }

        Servidor servidor;

        try { servidor = new Servidor(porta); }

        catch (Exception e) {

            System.err.println("[Servidor] Não foi possível abrir a porta " + porta + ": " + e.getMessage());

            System.exit(1);

            return;
        }

        servidor.iniciar();
    }



    public void iniciar () {

        LOG.registrar("", "SERVIDOR", "INICIO", "Aguardando registros na porta " + PROTOCOLO.getPortaLocal());

        System.out.println("[Servidor] Aguardando registros na porta " + PROTOCOLO.getPortaLocal());

        while (executando) {

            ResultadoRecebimento resultado;

            try { resultado = PROTOCOLO.receber(ESPERA_DO_LOOP_MS); }

            catch (IOException e) {

                if (!executando || PROTOCOLO.isFechado()) break;

                LOG.registrar("", "SERVIDOR", "ERRO", "Falha ao receber pacote: " + e.getMessage());

                continue;
            }

            switch (resultado.getTipo()) {

                case DATAGRAMA -> tratarDatagrama(resultado.getDATAGRAMA(), resultado.getORIGEM());

                case MALFORMADO -> LOG.registrar(texto(resultado.getORIGEM()), "SERVIDOR", "ERRO", "Datagrama invalido: " + resultado.getMotivo());

                case TIMEOUT -> { /* nenhum datagrama no intervalo: volta a aguardar */ }
            }
        }

        PROTOCOLO.fechar();

        LOG.registrar("", "SERVIDOR", "ENCERRAMENTO", "Servidor encerrado.");
    }



    /** Registra clientes e entrega cada datagrama ao atendimento do endereço de origem. */
    private void tratarDatagrama (Datagrama datagrama, InetSocketAddress remetente) {

        Comando comando = datagrama.getComando();

        if (comando == Comando.REGISTER) {

            registrarCliente(remetente, datagrama);

            return;
        }

        ClienteHandler atendimento = CLIENTES_ATIVOS.get(remetente);

        if (atendimento == null) atendimento = TRANSFERENCIAS_ATIVAS.get(remetente);

        if (atendimento != null) {

            atendimento.entregar(datagrama);

            return;
        }

        if (comando == Comando.UPLOAD || comando == Comando.DOWNLOAD) {

            iniciarTransferencia(remetente, datagrama);

            return;
        }

        // ACK, DATA e ERROR sem atendimento chegam depois do fim da operação e são ignorados.
        if (comando == Comando.LIST) enviarErro(remetente, "Cliente nao registrado. Envie REGISTER primeiro.");
    }



    private void registrarCliente (InetSocketAddress remetente, Datagrama datagrama) {

        if (CLIENTES_ATIVOS.containsKey(remetente)) {

            enviarAck(remetente);

            LOG.registrar(texto(remetente), "REGISTER", "DUPLICADO", "Cliente " + remetente + " ja registrado.");

            return;
        }

        ClienteHandler handler = new ClienteHandler(
                PROTOCOLO,
                remetente,
                GERENCIADOR_DE_ARQUIVOS,
                LOG,
                this,
                datagrama.getIdCliente(),
                false
        );

        CLIENTES_ATIVOS.put(remetente, handler);

        iniciarThread(handler, "ClienteHandler-" + remetente);

        enviarAck(remetente);

        LOG.registrar(texto(remetente), "REGISTER", "OK", "Cliente registrado: " + remetente + " (id=" + datagrama.getIdCliente() + ")");
    }



    /** Cria o atendimento de uma transferência que chegou de um endereço sem registro. */
    private void iniciarTransferencia (InetSocketAddress remetente, Datagrama primeiroDatagrama) {

        ClienteHandler handler = new ClienteHandler(
                PROTOCOLO,
                remetente,
                GERENCIADOR_DE_ARQUIVOS,
                LOG,
                this,
                "transferencia",
                true
        );

        TRANSFERENCIAS_ATIVAS.put(remetente, handler);

        handler.entregar(primeiroDatagrama);

        iniciarThread(handler, "Transferencia-" + remetente);
    }



    private static void iniciarThread (ClienteHandler handler, String nome) {

        Thread thread = new Thread(handler, nome);

        thread.setDaemon(true);

        thread.start();
    }



    private void enviarAck (InetSocketAddress destino) {

        // ACK do REGISTER: idTransferencia=0, numeroFragmento=0.
        enviar(destino, Datagrama.ack(0, 0), "ACK");
    }



    private void enviarErro (InetSocketAddress destino, String mensagem) {

        enviar(destino, Datagrama.erro(1, mensagem), "ERROR");
    }



    private void enviar (InetSocketAddress destino, Datagrama datagrama, String operacao) {

        try { PROTOCOLO.enviar(datagrama, destino); }

        catch (Exception e) {

            LOG.registrar(texto(destino), operacao, "ERRO", "Falha ao enviar para " + destino + ": " + e.getMessage());
        }
    }



    /** Remove o endereço dos clientes registrados e das transferências em andamento. */
    public void removerCliente (InetSocketAddress endereco) {

        ClienteHandler removido = CLIENTES_ATIVOS.remove(endereco);

        TRANSFERENCIAS_ATIVAS.remove(endereco);

        if (removido != null) LOG.registrar(texto(endereco), "CLIENTE", "REMOCAO", "Cliente removido: " + endereco);
    }



    /** Chamado pela ClienteHandler ao terminar: remove somente o próprio atendimento do endereço. */
    void removerAtendimento (InetSocketAddress endereco, ClienteHandler atendimento) {

        if (CLIENTES_ATIVOS.remove(endereco, atendimento)) LOG.registrar(texto(endereco), "CLIENTE", "REMOCAO", "Cliente removido: " + endereco);

        TRANSFERENCIAS_ATIVAS.remove(endereco, atendimento);
    }



    /** Itens "nome;tamanho", um por linha: o formato de LIST e UPDATE_LIST definido em comum.Datagrama. */
    String listaSerializada () throws IOException {

        return String.join("\n", GERENCIADOR_DE_ARQUIVOS.listarArquivosComTamanho());
    }



    public void notificarAtualizacaoLista () {

        try {

            Datagrama atualizacao = Datagrama.atualizarLista(listaSerializada());

            for (InetSocketAddress cliente : new ArrayList<>(CLIENTES_ATIVOS.keySet())) {

                try { PROTOCOLO.enviar(atualizacao, cliente); }

                catch (Exception e) {

                    LOG.registrar(texto(cliente), "UPDATE_LIST", "AVISO", "Falha ao enviar lista para " + cliente + ": " + e.getMessage());
                }
            }

            LOG.registrar("", "UPDATE_LIST", "OK", "Lista enviada para " + CLIENTES_ATIVOS.size() + " cliente(s).");
        }

        catch (Exception e) {

            LOG.registrar("", "UPDATE_LIST", "ERRO", "Falha ao montar a lista: " + e.getMessage());
        }
    }



    /** Gera um id de transferência único (int) para uploads/downloads. */
    public int novoIdTransferencia () {

        return CONTADOR_TRANSFERENCIAS.getAndIncrement();
    }



    public void encerrar () {

        executando = false;

        PROTOCOLO.fechar();
    }



    private static String texto (InetSocketAddress endereco) {

        if (endereco == null || endereco.getAddress() == null) return "";

        return endereco.getAddress().getHostAddress() + ":" + endereco.getPort();
    }



    public ProtocoloUDP getProtocolo () { return PROTOCOLO; }

    public GerenciadorArquivos getGerenciadorArquivos () { return GERENCIADOR_DE_ARQUIVOS; }

    public LogServidor getLog () { return LOG; }
}
