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
import java.util.UUID;

/**
 * ClienteHandler — thread que processa os comandos de um cliente registrado.
 *
 * Atende:
 * - LIST: devolve a lista atual de arquivos do repositório.
 * - UPLOAD: recebe fragmentos DATA, confirma cada um, remonta fora de ordem e valida o hash.
 * - DOWNLOAD: envia fragmentos numerados e reenvia os que ficarem sem ACK (máximo 5 tentativas).
 * - UPDATE_LIST: disparado via Servidor quando o repositório muda.
 *
 * API esperada das classes existentes:
 * - comum.Comando: enum com LIST, UPLOAD, DOWNLOAD, DATA, ACK, ERROR, UPDATE_LIST, REGISTER.
 * - comum.Datagrama: getComando(), getCampo(String), setCampo(String, String),
 *   setPayload(byte[]), getPayload(), serializar(), desserializar(byte[], int).
 * - SERVIDOR.GerenciadorArquivos: listarArquivos(), existe(String), lerArquivo(String),
 *   salvarArquivo(String nomeOriginal, byte[] dados) -> String nomeFinal.
 * - SERVIDOR.LogServidor: registrar(String origem, String operacao, String status, String detalhes).
 * - SERVIDOR.Servidor: notificarAtualizacaoLista().
 */
public class ClienteHandler implements Runnable {

    // Confiabilidade definida no projeto.
    
    private static final int TAMANHO_FRAGMENTO = 1024; // 1 KB, cabe no MTU Ethernet de 1500 bytes.
    
    private static final int TIMEOUT_MS = 2000;        // 2 s para retransmissão.
    
    private static final int MAX_TENTATIVAS = 5;       // tenta 5 vezes antes de abortar.

    // Nomes dos campos usados nos datagramas.
    
    private static final String CAMPO_NOME = "nome";
    
    private static final String CAMPO_TAMANHO = "tamanho";
    
    private static final String CAMPO_FRAGMENTOS = "fragmentos";
    
    private static final String CAMPO_HASH = "hash";
    
    private static final String CAMPO_TRANSFERENCIA = "transferencia";
    
    private static final String CAMPO_FRAGMENTO = "fragmento";
    
    private static final String CAMPO_LISTA = "lista";
    
    private static final String CAMPO_CODIGO = "codigo";
    
    private static final String CAMPO_MENSAGEM = "mensagem";
    
    private static final String CAMPO_NOME_FINAL = "nomeFinal";
    
    private static final String CAMPO_STATUS = "status";

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
            
                if (datagrama != null) processar(datagrama);
                
            }
            
            catch (SocketTimeoutException e) {

                System.out.println("Período de ociosidade: nenhum comando chegou nos últimos 2 s. Estamos mantendo a thread viva.");
            }
            
            catch (IOException e) {
             
                registrarLog("CONEXAO", "ERRO", "Falha de comunicacao: " + e.getMessage());
             
                break;
            }
        }
        
        encerrar();
    }
    
    

    private void processar (Datagrama datagrama) throws IOException {
    
        Comando comando = datagrama.getComando();
    
        if (comando == Comando.LIST) processarLista(datagrama);
        
        else if (comando == Comando.UPLOAD) processarUpload(datagrama);
        
        else if (comando == Comando.DOWNLOAD) baixar(datagrama);
        
        else if (comando == Comando.ERROR) registrarLog("CLIENTE", "ERRO", campo(datagrama, CAMPO_MENSAGEM));
        
        else enviarErro(datagrama, "1001", "Comando nao suportado: " + comando);
    }

    
    
    private void processarLista (Datagrama pedido) throws IOException {

        List<String> arquivos = GERENCIADOR.listarArquivos();

        String lista = String.join(";", arquivos);

        Datagrama resposta = new Datagrama(Comando.LIST);

        resposta.setCampo(CAMPO_LISTA, lista);

        enviar(resposta);

        registrarLog("LIST", "OK", arquivos.size() + " arquivo(s) no repositorio");
    }

    
    
    private void processarUpload (Datagrama pedido) throws IOException {
        
        String nomeOriginal = campo(pedido, CAMPO_NOME);
        
        String tamanhoStr = campo(pedido, CAMPO_TAMANHO);
        
        String fragmentosStr = campo(pedido, CAMPO_FRAGMENTOS);
        
        String hashInformado = campo(pedido, CAMPO_HASH);
        
        String transferencia = campo(pedido, CAMPO_TRANSFERENCIA);

        if (nomeOriginal.isEmpty() || tamanhoStr.isEmpty() || fragmentosStr.isEmpty() || hashInformado.isEmpty()) {
            
            enviarErro(pedido, "2001", "Campos obrigatorios ausentes no UPLOAD");
            
            return;
        }

        long tamanho;
        
        int totalFragmentos;
        
        try {
            tamanho = Long.parseLong(tamanhoStr);
        
            totalFragmentos = Integer.parseInt(fragmentosStr);
        }
        
        catch (NumberFormatException e) {
        
            enviarErro(pedido, "2002", "Tamanho ou numero de fragmentos invalido");
        
            return;
        }

        if (totalFragmentos < 0) {
            
            enviarErro(pedido, "2002", "Numero de fragmentos invalido");
            
            return;
        }

        if (transferencia.isEmpty()) transferencia = UUID.randomUUID().toString();

        // Buffer de fragmentos fora de ordem: chave = número do fragmento, valor = bytes recebidos.
        
        Map<Integer, byte[]> fragmentosRecebidos = new HashMap<>();
        
        Set<Integer> numerosConfirmados = new HashSet<>();

        // ACK inicial do UPLOAD: informa que o SERVIDOR está pronto para receber os DATA.
        enviarAck(transferencia, -1, "pronto");

        int esgotamentosConsecutivos = 0;

        while (numerosConfirmados.size() < totalFragmentos) {
        
            Datagrama datagrama;
        
            try {
                datagrama = receberDatagrama(TIMEOUT_MS);
            }
            
            catch (SocketTimeoutException e) {
            
                esgotamentosConsecutivos++;
            
                registrarLog("UPLOAD", "TIMEOUT", "Aguardando fragmentos (" + esgotamentosConsecutivos + "/" + MAX_TENTATIVAS + ")");
            
                if (esgotamentosConsecutivos >= MAX_TENTATIVAS) {
            
                    enviarErro(pedido, "2003", "Timeout aguardando fragmentos do subir");
            
                    registrarLog("UPLOAD", "ERRO", "Abortado por esgotar aguardando fragmentos");
            
                    return;
                }
            
                continue;
            }
            
            catch (IOException e) {
            
                enviarErro(pedido, "2004", "Falha de comunicacao durante subir: " + e.getMessage());
            
                registrarLog("UPLOAD", "ERRO", e.getMessage());
            
                return;
            }
            
            esgotamentosConsecutivos = 0;

            // Apenas DATA da transferência correta interessa nesse loop.
            
            if (datagrama.getComando() == Comando.DATA &&
            
                transferencia.equals(campo(datagrama, CAMPO_TRANSFERENCIA))) {
            
                int numero;
            
                try {
                    numero = Integer.parseInt(campo(datagrama, CAMPO_FRAGMENTO));
                }
                
                catch (NumberFormatException e) {
                
                    continue;
                }

                if (numero < 0 || numero >= totalFragmentos) {
                
                    enviarErro(pedido, "2005", "Fragmento fora dos limites: " + numero);
                
                    continue;
                }

                byte[] payload = datagrama.getPayload();
                
                if (payload == null) payload = new byte[0];

                if (fragmentosRecebidos.containsKey(numero)) {
                    
                    // Fragmento duplicado: descarta o payload e confirma novamente.
                    
                    enviarAck(transferencia, numero, "ok");
                    
                    continue;
                }

                // Armazena fragmento fora de ordem para remontar depois.
                
                fragmentosRecebidos.put(numero, payload);
                
                numerosConfirmados.add(numero);
                
                enviarAck(transferencia, numero, "ok");
            }
            
            else if (datagrama.getComando() == Comando.ERROR) {
            
                registrarLog("UPLOAD", "ERRO", "Cliente reportou: " + campo(datagrama, CAMPO_MENSAGEM));
            
                return;
            }
        }

        // Remonta em ordem sequencial e valida o tamanho.
        
        byte[] conteudo = remontar(fragmentosRecebidos, totalFragmentos, tamanho);
        
        if (conteudo == null) {
        
            enviarErro(pedido, "2006", "Remontagem falhou: tamanho dos fragmentos inconsistente");
        
            registrarLog("UPLOAD", "ERRO", "Remontagem falhou para " + nomeOriginal);
        
            return;
        }

        // Confere o hash calculado com o hash informado no registro da transferência.
        
        String hashCalculado = calcularHashSha256(conteudo);
        
        if (!hashCalculado.equalsIgnoreCase(hashInformado)) {
        
            enviarErro(pedido, "2007", "Hash do arquivo nao confere");
        
            registrarLog("UPLOAD", "ERRO", "Divergencia de hash para " + nomeOriginal + " (esperado " + hashInformado + ", calculado " + hashCalculado + ")");
            
            return;
        }
        
        

        String nomeFinal = GERENCIADOR.salvarArquivo(nomeOriginal, conteudo);
        
        registrarLog("UPLOAD", "OK", nomeOriginal + " salvo como " + nomeFinal + " (" + conteudo.length + " bytes)");

        // Notifica todos os clientes ativos de que o repositório mudou.
        
        SERVIDOR.notificarAtualizacaoLista();

        // Informa o nome final gravado, incluindo o sufixo de renomeação quando aplicado.
        
        Datagrama conclusao = new Datagrama(Comando.ACK);
        
        conclusao.setCampo(CAMPO_TRANSFERENCIA, transferencia);
        
        conclusao.setCampo(CAMPO_NOME_FINAL, nomeFinal);
        
        conclusao.setCampo(CAMPO_MENSAGEM, "Upload concluido");
        
        enviar(conclusao);
    }
    
    

    private void baixar (Datagrama pedido) {
        
        String nome = campo(pedido, CAMPO_NOME);
        
        String transferencia = campo(pedido, CAMPO_TRANSFERENCIA);

        if (nome.isEmpty()) {

            enviarErro(pedido, "3001", "Nome de arquivo nao informado");

            return;
        }

        if (transferencia.isEmpty()) transferencia = UUID.randomUUID().toString();

        if (!GERENCIADOR.existe(nome)) {

            enviarErro(pedido, "3002", "Arquivo inexistente: " + nome);

            registrarLog("DOWNLOAD", "ERRO", "Arquivo inexistente: " + nome);

            return;
        }

        byte[] conteudo = GERENCIADOR.lerArquivo(nome);

        int totalFragmentos = (int) Math.ceil(conteudo.length / (double) TAMANHO_FRAGMENTO);

        String hash = calcularHashSha256(conteudo);

        // Metadados + confirmação de que o baixar vai começar.

        Datagrama metadados = new Datagrama(Comando.ACK);

        metadados.setCampo(CAMPO_TRANSFERENCIA, transferencia);

        metadados.setCampo(CAMPO_NOME, nome);

        metadados.setCampo(CAMPO_TAMANHO, String.valueOf(conteudo.length));

        metadados.setCampo(CAMPO_FRAGMENTOS, String.valueOf(totalFragmentos));

        metadados.setCampo(CAMPO_HASH, hash);

        metadados.setCampo(CAMPO_STATUS, "OK");

        enviar(metadados);


        Set<Integer> acksRecebidos = new HashSet<>();

        int indice = 0;

        while (indice < totalFragmentos) {

            if (acksRecebidos.contains(indice)) {

                indice++;

                continue;
            }

            // Fragmento numerado sequencialmente para remontagem correta no cliente.

            byte[] parte = extrairFragmento(conteudo, indice);

            Datagrama data = new Datagrama(Comando.DATA);

            data.setCampo(CAMPO_TRANSFERENCIA, transferencia);

            data.setCampo(CAMPO_FRAGMENTO, String.valueOf(indice));

            data.setPayload(parte);

            boolean confirmado = false;

            int tentativa = 0;

            while (tentativa < MAX_TENTATIVAS && !confirmado) {

                enviar(data);

                registrarLog("DOWNLOAD", "ENVIO", "Fragmento " + indice + "/" + (totalFragmentos - 1) + " (tentativa " + (tentativa + 1) + ")");

                Datagrama ack = aguardarAck(transferencia, indice);

                if (ack != null) {

                    acksRecebidos.add(indice);

                    confirmado = true;
                }

                else {

                    tentativa++;

                    registrarLog("DOWNLOAD", "RETRANSMISSAO", "Fragmento " + indice + " sem ACK, reenviando (tentativa " + (tentativa + 1) + "/" + MAX_TENTATIVAS + ")");

                    if (tentativa >= MAX_TENTATIVAS) {

                        enviarErro(pedido, "4002", "Cliente nao confirmou os ACKs; transferencia abortada no fragmento " + indice);

                        registrarLog("DOWNLOAD", "ERRO", "Abortado apos " + MAX_TENTATIVAS + " tentativas no fragmento " + indice);

                        return;
                    }
                }
            }

            indice++;
        }

        // ACK final do baixar.

        Datagrama conclusao = new Datagrama(Comando.ACK);

        conclusao.setCampo(CAMPO_TRANSFERENCIA, transferencia);

        conclusao.setCampo(CAMPO_STATUS, "OK");

        conclusao.setCampo(CAMPO_MENSAGEM, "Download concluido");

        enviar(conclusao);

        registrarLog("DOWNLOAD", "OK", nome + " enviado (" + conteudo.length + " bytes em " + totalFragmentos + " fragmentos)");
    }



    /**
     * Espera até TIMEOUT_MS por um ACK do fragmento esperado. Retorna o datagrama de ACK
     * ou null se o tempo esgotar.
     */
    private Datagrama aguardarAck(String transferencia, int fragmentoEsperado) {

        long prazo = System.currentTimeMillis() + TIMEOUT_MS;

        while (System.currentTimeMillis() < prazo) {

            int restante = (int) (prazo - System.currentTimeMillis());

            try {

                Datagrama datagrama = receberDatagrama(Math.max(1, restante));

                if (datagrama.getComando() == Comando.ACK &&

                    transferencia.equals(campo(datagrama, CAMPO_TRANSFERENCIA)) &&

                    String.valueOf(fragmentoEsperado).equals(campo(datagrama, CAMPO_FRAGMENTO))) {

                    return datagrama;
                }

                // ACK de outro fragmento é ignorado aqui: a transferência é sequencial.
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



    /**
     * Extrai o fragmento de índice informado do conteúdo completo.
     */
    private byte[] extrairFragmento(byte[] conteudo, int indice) {

        int inicio = indice * TAMANHO_FRAGMENTO;

        int fim = Math.min(inicio + TAMANHO_FRAGMENTO, conteudo.length);

        return Arrays.copyOfRange(conteudo, inicio, fim);
    }



    /**
     * Remonta o arquivo em ordem sequencial a partir do buffer de fragmentos.
     * Retorna null se faltar algum fragmento ou se o tamanho final divergir.
     */
    private byte[] remontar(Map<Integer, byte[]> fragmentos, int totalFragmentos, long tamanhoEsperado) {

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        for (int i = 0; i < totalFragmentos; i++) {

            byte[] parte = fragmentos.get(i);

            if (parte == null) return null; // Fragmento perdido ou ausente.

            buffer.write(parte, 0, parte.length);
        }

        byte[] completo = buffer.toByteArray();

        if (completo.length != tamanhoEsperado) return null; // Tamanho inconsistente com o informado no UPLOAD.

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



    private void enviarAck (String transferencia, int fragmento, String status) {

        Datagrama ack = new Datagrama(Comando.ACK);

        ack.setCampo(CAMPO_TRANSFERENCIA, transferencia);

        ack.setCampo(CAMPO_FRAGMENTO, String.valueOf(fragmento));

        ack.setCampo(CAMPO_STATUS, status);

        enviar(ack);
    }



    private void enviarErro (Datagrama origem, String codigo, String mensagem) {

        Datagrama erro = new Datagrama(Comando.ERROR);

        erro.setCampo(CAMPO_CODIGO, codigo);

        erro.setCampo(CAMPO_MENSAGEM, mensagem);

        String transferencia = campo(origem, CAMPO_TRANSFERENCIA);

        if (!transferencia.isEmpty()) erro.setCampo(CAMPO_TRANSFERENCIA, transferencia);

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



    private Datagrama receberDatagrama (int timeoutMs) throws IOException {

        FANTOCHE.setSoTimeout(timeoutMs);

        byte[] buffer = new byte[65535];

        DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);

        FANTOCHE.receive(pacote);

        return Datagrama.desserializar(pacote.getData(), pacote.getLength());
    }



    private String campo(Datagrama datagrama, String nome) {

        String valor = datagrama.getCampo(nome);

        return valor == null ? "" : valor.trim();
    }



    private void registrarLog(String operacao, String status, String detalhes) {

        String origem = ENDERECO_DO_CLIENTE.getHostAddress() + ":" + PORTA_DO_CLIENTE;

        RELATORIO.registrar(origem, operacao, status, detalhes);
    }



    public void encerrar () {

        if (!ativo) return;

        ativo = false;

        registrarLog("CONEXAO", "OK", "Cliente " + ID_CLIENTE + " encerrado");

        if (FANTOCHE != null && !FANTOCHE.isClosed()) FANTOCHE.close();
    }
}