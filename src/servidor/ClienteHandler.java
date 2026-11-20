package servidor;

import comum.Comando;
import comum.Datagrama;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ClienteHandler — thread que processa os comandos de um cliente registrado.
 * Atende:
 * - LIST: devolve a lista atual de arquivos do repositório.
 * - UPLOAD: recebe fragmentos DATA, confirma cada um, remonta fora de ordem e valida o hash.
 * - DOWNLOAD: envia fragmentos numerados e reenvia os que ficarem sem ACK (máximo 5 tentativas).
 * - UPDATE_LIST: disparado via Servidor quando o repositório muda.
 */
public class ClienteHandler implements Runnable {

    private static final int TAMANHO_FRAGMENTO = 1024;

    private static final int TIMEOUT_MS = 2000;

    private static final int MAX_TENTATIVAS = 5;

    private final DatagramSocket FANTOCHE;

    private final InetAddress ENDERECO_DO_CLIENTE;

    private final int PORTA_DO_CLIENTE;

    private final GerenciadorArquivos GERENCIADOR;

    private final LogServidor RELATORIO;

    private final Servidor SERVIDOR;

    private final String ID_CLIENTE;

    private volatile boolean ativo = true;



    public ClienteHandler
      (DatagramSocket socket, InetAddress enderecoCliente, int portaCliente, GerenciadorArquivos gerenciador, LogServidor log, Servidor servidor, String idCliente)
            throws SocketException {

        FANTOCHE = socket;

        ENDERECO_DO_CLIENTE = enderecoCliente;

        PORTA_DO_CLIENTE = portaCliente;

        GERENCIADOR = gerenciador;

        RELATORIO = log;

        SERVIDOR = servidor;

        ID_CLIENTE = idCliente;

        FANTOCHE.setSoTimeout(TIMEOUT_MS);

        registrarLog("CONEXAO", "OK", "Cliente " + idCliente + " iniciado");
    }



    @Override public void run () {

        while (ativo) {

            try {
                Datagrama datagrama = receberDatagrama(TIMEOUT_MS);

                processar(datagrama);
            }

            catch (SocketTimeoutException e) {

                System.out.println("Período de ociosidade: nenhum comando chegou nos últimos 2 s. Thread mantida viva.");
            }

            catch (IOException e) {

                registrarLog("CONEXAO", "ERRO", "Falha de comunicacao: " + e.getMessage());

                break;
            }
        }

        encerrar();
    }



    private void processar(Datagrama datagrama) throws IOException {

        Comando comando = datagrama.getComando();

        if (comando == Comando.LIST) processarLista(datagrama);

        else if (comando == Comando.UPLOAD) processarUpload(datagrama);

        else if (comando == Comando.DOWNLOAD) baixar(datagrama);

        else if (comando == Comando.ERROR) registrarLog("CLIENTE", "ERRO", datagrama.getMensagemErro());

        else enviarErro(datagrama, "1001", "Comando nao suportado: " + comando);
    }



    private void processarLista(Datagrama pedido) throws IOException {

        List<String> arquivos = GERENCIADOR.listarArquivos();

        String lista = String.join(";", arquivos);

        Datagrama resposta = Datagrama.atualizarLista(lista);

        enviar(resposta);

        registrarLog("LIST", "OK", arquivos.size() + " arquivo(s) no repositorio");
    }



    private void processarUpload(Datagrama pedido) throws IOException {

        String nomeOriginal = pedido.getNome();

        long tamanho = pedido.getTamanhoArquivo();

        int totalFragmentos = pedido.getNumeroFragmentos();

        String hashInformado = pedido.getHash();

        if (nomeOriginal == null || nomeOriginal.isEmpty() || hashInformado == null || hashInformado.isEmpty()) {

            enviarErro(pedido, "2001", "Campos obrigatorios ausentes no UPLOAD");

            return;
        }

        if (totalFragmentos < 0) {

            enviarErro(pedido, "2002", "Numero de fragmentos invalido");

            return;
        }

        // Atribui um id de transferência único para esta operação.

        int idTransferencia = SERVIDOR.novoIdTransferencia();

        Map<Integer, byte[]> fragmentosRecebidos = new HashMap<>();

        Set<Integer> numerosConfirmados = new HashSet<>();

        // ACK inicial do UPLOAD: informa idTransferencia ao cliente.

        enviarAck(idTransferencia, 0);

        int esgotamentosConsecutivos = 0;

        while (numerosConfirmados.size() < totalFragmentos) {

            Datagrama datagrama;

            try { datagrama = receberDatagrama(TIMEOUT_MS); }

            catch (SocketTimeoutException e) {

                esgotamentosConsecutivos++;

                registrarLog("UPLOAD", "TIMEOUT", "Aguardando fragmentos (" + esgotamentosConsecutivos + "/" + MAX_TENTATIVAS + ")");

                if (esgotamentosConsecutivos >= MAX_TENTATIVAS) {

                    enviarErro(pedido, "2003", "Timeout aguardando fragmentos do upload");

                    registrarLog("UPLOAD", "ERRO", "Abortado por esgotar aguardando fragmentos");

                    return;
                }

                continue;
            }

            catch (IOException e) {

                enviarErro(pedido, "2004", "Falha de comunicacao durante upload: " + e.getMessage());

                registrarLog("UPLOAD", "ERRO", e.getMessage());

                return;
            }

            esgotamentosConsecutivos = 0;

            if ( (datagrama.getComando() == Comando.DATA) && (datagrama.getIdTransferencia() == idTransferencia) ) {

                int numero = datagrama.getNumeroFragmento();

                if (numero < 0 || numero >= totalFragmentos) {

                    enviarErro(pedido, "2005", "Fragmento fora dos limites: " + numero);

                    continue;
                }

                byte[] payload = datagrama.getPayload();

                if (payload == null) payload = new byte[0];

                if (fragmentosRecebidos.containsKey(numero)) {

                    // Duplicado: descarta e reconfirma.

                    enviarAck(idTransferencia, numero);

                    continue;
                }

                fragmentosRecebidos.put(numero, payload);

                numerosConfirmados.add(numero);

                enviarAck(idTransferencia, numero);
            }

            else if (datagrama.getComando() == Comando.ERROR) {

                registrarLog("UPLOAD", "ERRO", "Cliente reportou: " + datagrama.getMensagemErro());

                return;
            }
        }

        // Remonta e valida.

        byte[] conteudo = remontar(fragmentosRecebidos, totalFragmentos, tamanho);

        if (conteudo == null) {

            enviarErro(pedido, "2006", "Remontagem falhou: tamanho dos fragmentos inconsistente");

            registrarLog("UPLOAD", "ERRO", "Remontagem falhou para " + nomeOriginal);

            return;
        }

        String hashCalculado = calcularHashSha256(conteudo);

        if (!hashCalculado.equalsIgnoreCase(hashInformado)) {

            enviarErro(pedido, "2007", "Hash do arquivo nao confere");

            registrarLog("UPLOAD", "ERRO", "Divergencia de hash para " + nomeOriginal);

            return;
        }

        String nomeFinal = GERENCIADOR.salvarArquivo(nomeOriginal, conteudo);

        registrarLog("UPLOAD", "OK", nomeOriginal + " salvo como " + nomeFinal + " (" + conteudo.length + " bytes)");

        SERVIDOR.notificarAtualizacaoLista();

        // ACK final de conclusão (numeroFragmento=-1 sinaliza fim).

        Datagrama conclusao = Datagrama.ack(idTransferencia, -1);

        enviar(conclusao);
    }



    private void baixar(Datagrama pedido) {

        String nome = pedido.getNome();

        if (nome == null || nome.isEmpty()) {

            enviarErro(pedido, "3001", "Nome de arquivo nao informado");

            return;
        }

        int idTransferencia = SERVIDOR.novoIdTransferencia();

        if (!GERENCIADOR.existe(nome)) {

            enviarErro(pedido, "3002", "Arquivo inexistente: " + nome);

            registrarLog("DOWNLOAD", "ERRO", "Arquivo inexistente: " + nome);

            return;
        }

        byte[] conteudo = GERENCIADOR.lerArquivo(nome);

        int totalFragmentos = (int) Math.ceil(conteudo.length / (double) TAMANHO_FRAGMENTO);

        String hash = calcularHashSha256(conteudo);

        // Envia metadados como UPLOAD (reutiliza o layout: nome, tamanho, nº fragmentos, hash).

        Datagrama metadados = Datagrama.subir(nome, conteudo.length, totalFragmentos, hash)
                .comIdTransferencia(idTransferencia);
        enviar(metadados);

        Set<Integer> acksRecebidos = new HashSet<>();

        int indice = 0;

        while (indice < totalFragmentos) {

            if (acksRecebidos.contains(indice)) {

                indice++;

                continue;
            }

            byte[] parte = extrairFragmento(conteudo, indice);

            Datagrama data = Datagrama.dados(idTransferencia, indice, parte);

            boolean confirmado = false;

            int tentativa = 0;

            while (tentativa < MAX_TENTATIVAS && !confirmado) {

                enviar(data);

                registrarLog("DOWNLOAD", "ENVIO", "Fragmento " + indice + "/" + (totalFragmentos - 1) + " (tentativa " + (tentativa + 1) + ")");

                Datagrama ack = aguardarAck(idTransferencia, indice);

                if (ack != null) {

                    acksRecebidos.add(indice);

                    confirmado = true;
                }

                else {
                    tentativa++;

                    registrarLog("DOWNLOAD", "RETRANSMISSAO", "Fragmento " + indice + " sem ACK, reenviando (tentativa " + (tentativa + 1) + "/" + MAX_TENTATIVAS + ")");

                    if (tentativa >= MAX_TENTATIVAS) {

                        enviarErro(pedido, "4002", "Cliente nao confirmou ACKs; transferencia abortada no fragmento " + indice);

                        registrarLog("DOWNLOAD", "ERRO", "Abortado apos " + MAX_TENTATIVAS + " tentativas no fragmento " + indice);

                        return;
                    }
                }
            }

            indice++;
        }

        // ACK final de conclusão.

        Datagrama conclusao = Datagrama.ack(idTransferencia, -1);

        enviar(conclusao);

        registrarLog("DOWNLOAD", "OK", nome + " enviado (" + conteudo.length + " bytes em " + totalFragmentos + " fragmentos)");
    }



    private Datagrama aguardarAck(int transferencia, int fragmentoEsperado) {

        long prazo = System.currentTimeMillis() + TIMEOUT_MS;

        while (System.currentTimeMillis() < prazo) {

            int restante = (int) (prazo - System.currentTimeMillis());


            try {

                Datagrama datagrama = receberDatagrama(Math.max(1, restante));

                if (datagrama.getComando() == Comando.ACK &&
                        datagrama.getIdTransferencia() == transferencia &&
                        datagrama.getNumeroFragmento() == fragmentoEsperado) {
                    return datagrama;
                }
            }

            catch (SocketTimeoutException e) {

                return null;
            }

            catch (IOException e) {

                return null;
            }
        }

        return null;
    }



    private byte[] extrairFragmento(byte[] conteudo, int indice) {

        int inicio = indice * TAMANHO_FRAGMENTO;

        int fim = Math.min(inicio + TAMANHO_FRAGMENTO, conteudo.length);

        return Arrays.copyOfRange(conteudo, inicio, fim);
    }



    private byte[] remontar(Map<Integer, byte[]> fragmentos, int totalFragmentos, long tamanhoEsperado) {

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        for (int i = 0; i < totalFragmentos; i++) {

            byte[] parte = fragmentos.get(i);

            if (parte == null) return null;

            buffer.write(parte, 0, parte.length);
        }

        byte[] completo = buffer.toByteArray();

        if (completo.length != tamanhoEsperado) return null;

        return completo;
    }



    private String calcularHashSha256(byte[] dados) {

        try {

            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            byte[] hash = digest.digest(dados);

            StringBuilder hex = new StringBuilder();

            for (byte b : hash) hex.append(String.format("%02x", b & 0xff));

            return hex.toString();
        }

        catch (NoSuchAlgorithmException e) {

            return "";
        }
    }



    private void enviarAck(int idTransferencia, int numeroFragmento) {

        Datagrama ack = Datagrama.ack(idTransferencia, numeroFragmento);

        enviar(ack);
    }



    private void enviarErro(Datagrama origem, String codigo, String mensagem) {

        int codigoInt;

        try { codigoInt = Integer.parseInt(codigo); } catch (NumberFormatException e) { codigoInt = 0; }

        Datagrama erro = Datagrama.erro(codigoInt, mensagem);

        enviar(erro);
    }



    private void enviar(Datagrama datagrama) {

        try {

            byte[] bytes = datagrama.serialize();

            DatagramPacket pacote = new DatagramPacket(bytes, bytes.length, ENDERECO_DO_CLIENTE, PORTA_DO_CLIENTE);

            FANTOCHE.send(pacote);
        }

        catch (IOException e) {

            registrarLog("ENVIO", "ERRO", "Falha ao enviar datagrama: " + e.getMessage());
        }
    }



    private Datagrama receberDatagrama(int timeoutMs) throws IOException {

        FANTOCHE.setSoTimeout(timeoutMs);

        byte[] buffer = new byte[65535];

        DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);

        FANTOCHE.receive(pacote);

        byte[] dados = Arrays.copyOf(pacote.getData(), pacote.getLength());

        return Datagrama.parse(dados);
    }



    private void registrarLog(String operacao, String status, String detalhes) {

        String origem = ENDERECO_DO_CLIENTE.getHostAddress() + ":" + PORTA_DO_CLIENTE;

        RELATORIO.registrar(origem, operacao, status, detalhes);
    }



    public void encerrar() {

        if (!ativo) return;

        ativo = false;

        registrarLog("CONEXAO", "OK", "Cliente " + ID_CLIENTE + " encerrado");

        if (FANTOCHE != null && !FANTOCHE.isClosed()) FANTOCHE.close();
    }
}