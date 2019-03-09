/*
 * Copyright (c) 2018 Tarokun LLC. All rights reserved.
 *
 * This file is part of BlobBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.blobbase.utils;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * SHA1 utilities
*/
public class Sha1Util
{
    /**
     * Create a hash from an integer.
     * 
     * @param i
     * @return hash bytes
     */
    public static byte[] hashInt(int i)
    {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(i);

        byte[] result = b.array();
        result = hashBytes(result);

        return result;
    }

    /**
     * Create a hash from buffer provided.
     * 
     * @param buffer of input bytes
     * @return hash bytes
     */
    public static byte[] hashBytes(byte[] buffer)
    {
        byte[] digest = null;
        try
        {
            MessageDigest crypt = MessageDigest.getInstance("SHA-1");
            crypt.reset();
            crypt.update(buffer);
            digest = crypt.digest();
        } catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
        return digest;
    }
 
}
