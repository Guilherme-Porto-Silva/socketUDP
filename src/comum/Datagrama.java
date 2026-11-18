package comum;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Define o contrato binário único dos datagramas do sistema.
 *
 * Cabeçalho fixo (todos os números em big-endian):
 *
 * +---------------------------------------------------------------+
 * | byte 0 | versão do protocolo = 1                              |
 * | byte 1 | código do COMANDO (Comando)                          |
 * | ...    | campos específicos do COMANDO                        |
 * +---------------------------------------------------------------+
 *
 * Strings são sempre serializadas como:
 *   [int 4 bytes - comprimento em bytes][bytes UTF-8]
 *
 * Campos numéricos usam a codificação big-endian do DataOutputStream
 * (long = 8 bytes, int = 4 bytes).
 *
 * Layout por COMANDO, após o cabeçalho:
 *
 * REGISTER    : idCliente (String)
 * LIST        : (nenhum campo)
 * UPLOAD      : nome (String)
 *               tamanhoArquivo (long 8 bytes)
 *               numeroFragmentos (int 4 bytes)
 *               hash (String)
 * DATA        : idTransferencia (int 4 bytes)
 *               numeroFragmento (int 4 bytes)
 *               comprimentoPayload (int 4 bytes)
 *               payload (1 a 1024 bytes)
 * DOWNLOAD    : nome (String)
 * UPDATE_LIST : listaArquivos (String)
 * ACK         : idTransferencia (int 4 bytes)
 *               numeroFragmento (int 4 bytes)
 *               - ACK de COMANDO:  idTransferencia = 0, numeroFragmento = 0
 *               - ACK de UPLOAD :  idTransferencia = id atribuído, numeroFragmento = 0
 *               - ACK de DATA   :  idTransferencia = id da transferência, numeroFragmento = fragmento confirmado
 * ERROR       : codigoErro (int 4 bytes)
 *               mensagem (String)
 *
 * A lista de arquivos (LIST e UPDATE_LIST) é uma String UTF-8 em que cada
 * arquivo ocupa uma linha no formato "nome;tamanho", por exemplo:
 *
 *   foto.png;2048
 *   relatorio.pdf;1048576
 *
 * Integridade: o campo hash de UPLOAD contém o SHA-256 do arquivo completo,
 * codificado em hexadecimal com 64 caracteres. Após a remontagem no servidor,
 * o hash deve ser recalculado com calcularHashSha256() e comparado com
 * hashConfere().
 *
 * Este arquivo é a única fonte do formato dos datagramas. As demais classes
 * do projeto devem usar Datagrama.serialize() e Datagrama.parse().
 */
public final class Datagrama {

    public static final byte VERSAO_PROTOCOLO = 1;

//  Limites de segurança para evitar alocações descontroladas em datagramas malformados.
    
    public static final int TAMANHO_MAXIMO_STRING = 10 * 1024 * 1024;
    
    public static final int TAMANHO_MAXIMO_PAYLOAD = Fragmento.TAMANHO_MAXIMO;

//  Códigos de erro não definidos na tabela do protocolo; adotados aqui para padronizar as mensagens ERROR. As classes de aplicação podem reutilizá-los.

    public static final int CODIGO_ERRO_GENERICO = 0;

    public static final int CODIGO_ARQUIVO_INEXISTENTE = 1;

    public static final int CODIGO_HASH_DIVERGENTE = 2;

    public static final int CODIGO_TRANSFERENCIA_ABORTADA = 3;

    private static final Pattern PADRAO_HASH_SHA256 = Pattern.compile("[0-9a-fA-F]{64}");

    private final Comando COMANDO;

    private String idCliente;

    private String nome;

    private long tamanhoArquivo;

    private int numeroFragmentos;

    private String hash;

    private int idTransferencia;

    private int numeroFragmento;

    private byte[] payload;

    private String listaArquivos;

    private int codigoErro;

    private String mensagemErro;



    public Datagrama(Comando comando) {

        if (comando == null) throw new IllegalArgumentException("COMANDO não pode ser nulo");

        this.COMANDO = comando;
    }



    public static Datagrama registrar (String idCliente) {

        if (idCliente == null || idCliente.trim().isEmpty()) throw new IllegalArgumentException("idCliente não pode ser vazio");

        Datagrama datagrama = new Datagrama(Comando.REGISTER);

        datagrama.idCliente = idCliente;

        return datagrama;
    }



    public static Datagrama listar () {

        return new Datagrama(Comando.LIST);
    }



    public static Datagrama subir (String nome, long tamanhoArquivo, int numeroFragmentos, String hashSha256) {

        if (nome == null || nome.trim().isEmpty()) throw new IllegalArgumentException("nome de arquivo não pode ser vazio");

        if (tamanhoArquivo < 0) throw new IllegalArgumentException("tamanho de arquivo não pode ser negativo");

        if (numeroFragmentos < 0) throw new IllegalArgumentException("numeroFragmentos não pode ser negativo");

        if (hashSha256 == null || !PADRAO_HASH_SHA256.matcher(hashSha256).matches())

            throw new IllegalArgumentException("hash deve ter 64 caracteres hexadecimais");

        Datagrama datagrama = new Datagrama(Comando.UPLOAD);

        datagrama.nome = nome;

        datagrama.tamanhoArquivo = tamanhoArquivo;

        datagrama.numeroFragmentos = numeroFragmentos;

        datagrama.hash = hashSha256;

        return datagrama;
    }



    public static Datagrama dados (int idTransferencia, int numeroFragmento, byte[] payload) {

        if (idTransferencia < 0) throw new IllegalArgumentException("idTransferencia não pode ser negativo");

        if (numeroFragmento < 0) throw new IllegalArgumentException("numeroFragmento não pode ser negativo");

        if (payload == null || payload.length == 0 || payload.length > TAMANHO_MAXIMO_PAYLOAD)

            throw new IllegalArgumentException("payload deve ter entre 1 e " + TAMANHO_MAXIMO_PAYLOAD + " bytes");

        Datagrama datagrama = new Datagrama(Comando.DATA);

        datagrama.idTransferencia = idTransferencia;

        datagrama.numeroFragmento = numeroFragmento;

        datagrama.payload = Arrays.copyOf(payload, payload.length);

        return datagrama;
    }



    public static Datagrama dados (Fragmento fragmento) {

        if (fragmento == null) throw new IllegalArgumentException("fragmento não pode ser nulo");

        return dados(fragmento.getIdTransferencia(), fragmento.getNumeroFragmento(), fragmento.getPayload());
    }



    public static Datagrama baixar (String nome) {

        if (nome == null || nome.trim().isEmpty()) throw new IllegalArgumentException("nome de arquivo não pode ser vazio");

        Datagrama datagrama = new Datagrama(Comando.DOWNLOAD);

        datagrama.nome = nome;

        return datagrama;
    }



    public static Datagrama atualizarLista(String listaArquivos) {

        if (listaArquivos == null) throw new IllegalArgumentException("listaArquivos não pode ser nula");

        Datagrama datagrama = new Datagrama(Comando.UPDATE_LIST);

        datagrama.listaArquivos = listaArquivos;

        return datagrama;
    }



    public static Datagrama ack (int idTransferencia, int numeroFragmento) {

        if (idTransferencia < 0 || numeroFragmento < 0) throw new IllegalArgumentException("idTransferencia e numeroFragmento devem ser >= 0");

        Datagrama datagrama = new Datagrama(Comando.ACK);

        datagrama.idTransferencia = idTransferencia;

        datagrama.numeroFragmento = numeroFragmento;

        return datagrama;
    }



    public static Datagrama erro (int codigoErro, String mensagem) {

        if (mensagem == null) throw new IllegalArgumentException("mensagem de erro não pode ser nula");

        Datagrama datagrama = new Datagrama(Comando.ERROR);

        datagrama.codigoErro = codigoErro;

        datagrama.mensagemErro = mensagem;

        return datagrama;
    }



    public Comando getComando () { return COMANDO; }

    public byte getVersao () { return VERSAO_PROTOCOLO; }

    public String getIdCliente () { return idCliente; }

    public String getNome () { return nome; }

    public long getTamanhoArquivo () { return tamanhoArquivo; }

    public int getNumeroFragmentos () { return numeroFragmentos; }

    public String getHash () { return hash; }

    public int getIdTransferencia () { return idTransferencia; }

    public int getNumeroFragmento () { return numeroFragmento; }

    public byte[] getPayload () {
        
        return payload == null ? null : Arrays.copyOf(payload, payload.length);
    }



    public String getListaArquivos () { return listaArquivos; }

    public int getCodigoErro () { return codigoErro; }

    public String getMensagemErro () { return mensagemErro; }

    
    
    public Fragmento getFragmento() {
        
        if (COMANDO != Comando.DATA) throw new IllegalStateException("getFragmento() só é válido para datagramas DATA");
        
        if (payload == null) throw new IllegalStateException("payload não inicializado");
        
        return new Fragmento(idTransferencia, numeroFragmento, payload);
    }
    
    

    public byte[] serialize () {
        
        validarCampos();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        try (DataOutputStream out = new DataOutputStream(baos)) {
        
            out.writeByte(VERSAO_PROTOCOLO);
        
            out.writeByte(COMANDO.getCodigo());

            
            
            switch (COMANDO) {
        
                case REGISTER:
                    
                    escreverString(out, idCliente);
                    
                    break;
        
                case LIST:
                    
                    break;
        
                case UPLOAD:
                    
                    escreverString(out, nome);
                    
                    out.writeLong(tamanhoArquivo);
                    
                    out.writeInt(numeroFragmentos);
                    
                    escreverString(out, hash);
                    
                    break;
        
                case DATA:
                    
                    out.writeInt(idTransferencia);
                    
                    out.writeInt(numeroFragmento);
                    
                    out.writeInt(payload.length);
                    
                    out.write(payload);
                    
                    break;
        
                case DOWNLOAD:
                    
                    escreverString(out, nome);
                    
                    break;
                    
                case UPDATE_LIST:
                    
                    escreverString(out, listaArquivos);
                    
                    break;
                    
                case ACK:
                    
                    out.writeInt(idTransferencia);
                    
                    out.writeInt(numeroFragmento);
                    
                    break;
                    
                case ERROR:
                    
                    out.writeInt(codigoErro);
                    
                    escreverString(out, mensagemErro);
                    
                    break;
                    
                default:
                    
                    throw new IllegalStateException("Comando sem serialização implementada: " + COMANDO);
            }
            
        }
        
        catch (IOException e) {
        
            throw new IllegalStateException("Falha ao serialize datagrama: " + e.getMessage(), e);
        }

        return baos.toByteArray();
    }

    
    
    public static Datagrama parse (byte[] dados) {
        
        if (dados == null || dados.length < 2) throw new IllegalArgumentException("Datagrama inválido: cabeçalho incompleto");

        ByteArrayInputStream bais = new ByteArrayInputStream(dados);
        
        try (DataInputStream in = new DataInputStream(bais)) {
        
            byte versao = in.readByte();
        
            if (versao != VERSAO_PROTOCOLO) throw new IllegalArgumentException("Versão de protocolo não suportada: " + versao);

            byte codigo = in.readByte();
            
            Comando comando = Comando.fromCodigo(codigo);
            
            Datagrama datagrama = new Datagrama(comando);

            
            
            switch (comando) {
            
                case REGISTER:
                    
                    datagrama.idCliente = lerString(in);
                    
                    break;
                    
                case LIST:
                    
                    break;
                    
                case UPLOAD:
                    
                    datagrama.nome = lerString(in);
                    
                    datagrama.tamanhoArquivo = in.readLong();
                    
                    datagrama.numeroFragmentos = in.readInt();
                    
                    datagrama.hash = lerString(in);
                    
                    break;
                    
                case DATA:
                    
                    datagrama.idTransferencia = in.readInt();
                    
                    datagrama.numeroFragmento = in.readInt();
                    
                    int tamanhoPayload = in.readInt();
                    
                    if (tamanhoPayload <= 0 || tamanhoPayload > TAMANHO_MAXIMO_PAYLOAD) throw new IllegalArgumentException("Tamanho de payload inválido para DATA: " + tamanhoPayload);
                    
                    byte[] payload = new byte[tamanhoPayload];
                    
                    in.readFully(payload);
                    
                    datagrama.payload = payload;
                    
                    break;
                    
                case DOWNLOAD:
                    
                    datagrama.nome = lerString(in);
                    
                    break;
                    
                case UPDATE_LIST:
                    
                    datagrama.listaArquivos = lerString(in);
                    
                    break;
                    
                case ACK:
                    
                    datagrama.idTransferencia = in.readInt();
                    
                    datagrama.numeroFragmento = in.readInt();
                    
                    break;
                    
                case ERROR:
                    
                    datagrama.codigoErro = in.readInt();
                    
                    datagrama.mensagemErro = lerString(in);
                    
                    break;
                    
                default:
                    
                    throw new IllegalArgumentException("Comando não implementado no parse: " + comando);
            }

            
            
            datagrama.validarCampos();

            if (bais.available() != 0) throw new IllegalArgumentException("Datagrama contém bytes não esperados após o COMANDO");

            return datagrama;
        }
        
        catch (EOFException e) {
        
            throw new IllegalArgumentException("Datagrama truncado: " + e.getMessage(), e);
        }
        
        catch (IOException e) {
        
            throw new IllegalArgumentException("Erro ao ler datagrama: " + e.getMessage(), e);
        }
    }
    
    

    private void validarCampos () {
    
        switch (COMANDO) {
            
            case REGISTER:
                
                if (idCliente == null || idCliente.trim().isEmpty()) throw new IllegalArgumentException("REGISTER exige idCliente não vazio");
                
                break;
                
            case LIST:
                
                break;
                
            case UPLOAD:
                
                if (nome == null || nome.trim().isEmpty()) throw new IllegalArgumentException("UPLOAD exige nome de arquivo");
                
                if (tamanhoArquivo < 0) throw new IllegalArgumentException("tamanho de arquivo não pode ser negativo");
                
                if (numeroFragmentos < 0) throw new IllegalArgumentException("numeroFragmentos não pode ser negativo");
                
                if (hash == null || !PADRAO_HASH_SHA256.matcher(hash).matches()) throw new IllegalArgumentException("UPLOAD exige hash SHA-256 em hexadecimal de 64 caracteres");
                
                break;
                
            case DATA:
                
                if (idTransferencia < 0) throw new IllegalArgumentException("DATA exige idTransferencia >= 0");
                
                if (numeroFragmento < 0) throw new IllegalArgumentException("DATA exige numeroFragmento >= 0");
                
                if (payload == null || payload.length == 0 || payload.length > TAMANHO_MAXIMO_PAYLOAD) throw new IllegalArgumentException("DATA exige payload entre 1 e " + TAMANHO_MAXIMO_PAYLOAD + " bytes");
                
                break;
                
            case DOWNLOAD:
                
                if (nome == null || nome.trim().isEmpty()) throw new IllegalArgumentException("DOWNLOAD exige nome de arquivo");
                
                break;
                
            case UPDATE_LIST:
                
                if (listaArquivos == null) throw new IllegalArgumentException("UPDATE_LIST exige listaArquivos não nula");
                
                break;
                
            case ACK:
                
                if (idTransferencia < 0 || numeroFragmento < 0) throw new IllegalArgumentException("ACK exige idTransferencia e numeroFragmento >= 0");
                
                break;
                
            case ERROR:
                
                if (mensagemErro == null) throw new IllegalArgumentException("ERROR exige mensagem não nula");
                
                break;
                
            default:
                
                throw new IllegalStateException("Comando sem validação definida: " + COMANDO);
        }
    }
    
    

    private static void escreverString (DataOutputStream out, String valor) throws IOException {
    
        byte[] bytes = valor.getBytes(StandardCharsets.UTF_8);
    
        if (bytes.length > TAMANHO_MAXIMO_STRING) throw new IOException("String excede o limite de " + TAMANHO_MAXIMO_STRING + " bytes");
        
        out.writeInt(bytes.length);
        
        out.write(bytes);
    }
    
    

    private static String lerString (DataInputStream in) throws IOException {
    
        int tamanho = in.readInt();
    
        if (tamanho < 0 || tamanho > TAMANHO_MAXIMO_STRING) throw new IOException("Tamanho de string inválido: " + tamanho);
        
        byte[] bytes = new byte[tamanho];
        
        in.readFully(bytes);
        
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    

    public static String calcularHashSha256 (byte[] dados) {
    
        if (dados == null) throw new IllegalArgumentException("dados não pode ser nulo");
        
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
        
            byte[] hash = digest.digest(dados);
        
            StringBuilder sb = new StringBuilder(hash.length * 2);
        
            for (byte b : hash) {
        
                String hex = Integer.toHexString(b & 0xff);
        
                if (hex.length() == 1) sb.append('0');
                
                sb.append(hex);
            }
            
            return sb.toString();
        }
        
        catch (NoSuchAlgorithmException e) {
        
            throw new IllegalStateException("SHA-256 não disponível na JVM", e);
        }
    }
    
    

    public static boolean hashConfere (byte[] dados, String hashEsperado) {
    
        if (dados == null || hashEsperado == null) return false;
        
        if (!PADRAO_HASH_SHA256.matcher(hashEsperado).matches()) return false;
        
        return calcularHashSha256(dados).equalsIgnoreCase(hashEsperado);
    }
}